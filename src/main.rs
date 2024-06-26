use std::{
    fmt::Display,
    io,
    path::{Path, PathBuf},
    sync::Arc,
};

use dashmap::DashMap;
use flate2::{Compress, Compression, Status};
use rocket::{
    build,
    fs::{
        dir_root, filter_dotfiles, index, normalize_dirs, File, FileResponse, FileServer, Rewriter,
    },
    http::{ContentType, Header},
    launch,
    tokio::io::{AsyncReadExt, AsyncWriteExt},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Algorithm {
    Gzip,
}

impl Algorithm {
    fn name(&self) -> &'static str {
        match self {
            Algorithm::Gzip => "gzip",
        }
    }

    fn from_name(name: &str) -> Option<Self> {
        match name {
            "gzip" => Some(Self::Gzip),
            _ => None,
        }
    }
}

impl Display for Algorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

struct Info {
    compressions: Vec<Algorithm>,
    pending: Vec<Algorithm>,
}

struct CachedCompression {
    map: Arc<DashMap<PathBuf, Info>>,
}

impl CachedCompression {
    fn new() -> Self {
        Self {
            map: Arc::new(DashMap::new()),
        }
    }

    fn get_valid(&self, req: &rocket::Request<'_>) -> Option<Algorithm> {
        req.headers()
            .get("Accept-Encoding")
            .flat_map(|v| v.split(|c| c == ','))
            .filter_map(|coding| {
                // TODO: Parsing Hack. Filters everything after the `;`, and the whole item if `q = 0`
                // Will select client's preferred, assume the client placed them in preferred order.
                if let Some((name, params)) = coding.split_once(';') {
                    if let Some((param_name, val)) = params.split_once('=') {
                        let val: f32 = val.trim().parse().unwrap_or(0.);
                        if val != 0. || param_name.trim() != "q" {
                            Some(name.trim())
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                } else {
                    Some(coding.trim())
                }
            })
            .filter_map(|coding| Algorithm::from_name(coding))
            .nth(0)
    }

    fn dispatch(&self, algo: Algorithm, path: PathBuf) {
        let map = self.map.clone();
        rocket::tokio::spawn(async move {
            {
                let mut v = map.entry(path.clone()).or_insert(Info {
                    compressions: vec![],
                    pending: vec![],
                });
                if v.pending.contains(&algo) {
                    return;
                }
                v.pending.push(algo);
                drop(v);
            }
            let new_name = format!("{}.{algo}", path.file_name().unwrap().to_str().unwrap());
            let new_path = path.with_file_name(new_name);
            let compressor = match algo {
                Algorithm::Gzip => Compress::new_gzip(Compression::new(9), 15),
            };

            let success = match Self::compress(compressor, &path, &new_path).await {
                Ok(()) => true,
                Err(_e) => {
                    // TODO: log error
                    println!("Error: {_e:?}");
                    false
                }
            };
            {
                let mut v = map.entry(path.clone()).or_insert(Info {
                    compressions: vec![],
                    pending: vec![],
                });
                v.pending.retain(|a| *a != algo);
                if success {
                    v.compressions.push(algo);
                }
                drop(v);
            }
        });
    }

    async fn compress(mut compressor: Compress, path: &Path, new_path: &Path) -> io::Result<()> {
        // This isn't the ideal API to be using, but flate2 only provides sync APIs, so I have to
        // deal with the async files for it.
        let mut input = rocket::tokio::fs::File::open(path).await?;
        let mut output = rocket::tokio::fs::File::create(new_path).await?;
        let mut input_buf = [0u8; 1024];
        let mut output_buf = [0u8; 1024];
        loop {
            let size = input.read(&mut input_buf).await?;
            if size == 0 {
                loop {
                    let start_out = compressor.total_out();
                    match compressor.compress(&[], &mut output_buf, flate2::FlushCompress::Finish) {
                        Ok(Status::Ok) => {
                            let out_size = compressor.total_out() - start_out;

                            output.write_all(&output_buf[..out_size as usize]).await?;
                        }
                        Ok(Status::BufError) => {
                            return Err(io::Error::new(io::ErrorKind::InvalidData, ""))
                        }
                        Ok(Status::StreamEnd) => break,
                        Err(_) => return Err(io::Error::new(io::ErrorKind::InvalidData, "")),
                    }
                }
                break;
            }
            let mut rem = &input_buf[..size];
            while rem.len() > 0 {
                let start_in = compressor.total_in();
                let start_out = compressor.total_out();
                match compressor.compress(rem, &mut output_buf, flate2::FlushCompress::None) {
                    Ok(Status::Ok) => {
                        let in_size = compressor.total_in() - start_in;
                        let out_size = compressor.total_out() - start_out;

                        output.write_all(&output_buf[..out_size as usize]).await?;
                        rem = &rem[in_size as usize..];
                    }
                    Ok(Status::BufError) => {
                        return Err(io::Error::new(io::ErrorKind::InvalidData, ""))
                    }
                    Ok(Status::StreamEnd) => todo!("This should never happen when compressing"),
                    Err(_) => return Err(io::Error::new(io::ErrorKind::InvalidData, "")),
                }
            }
        }
        // Note: this will only be executed if the above succeeds.
        Ok(())
    }
}

// This might be a good addition to `ContentType` itself
fn content_type_from_path(path: impl AsRef<Path>) -> Option<ContentType> {
    ContentType::from_extension(path.as_ref().extension()?.to_str()?)
}

impl Rewriter for CachedCompression {
    fn rewrite<'p, 'h>(
        &self,
        path: Option<FileResponse<'p, 'h>>,
        req: &rocket::Request<'_>,
    ) -> Option<FileResponse<'p, 'h>> {
        match path {
            Some(FileResponse::File(File {
                mut path,
                mut headers,
            })) => {
                if let Some(algo) = self.get_valid(req) {
                    if self
                        .map
                        .get(path.as_ref())
                        .is_some_and(|info| info.compressions.contains(&algo))
                    {
                        // Since we change the path, it seems like we override any
                        // automatic content-type detection, so we just do it manually
                        // We could implement this directly on File as well
                        if let Some(ct) = content_type_from_path(&path) {
                            headers.add(ct);
                        }
                        headers.add(Header::new("Content-Encoding", algo.to_string()));
                        // TODO: this unwraps a bunch of errors, that should be handled
                        let new_name =
                            format!("{}.{algo}", path.file_name().unwrap().to_str().unwrap());
                        path.to_mut().set_file_name(new_name);
                    } else {
                        self.dispatch(algo, path.clone().into_owned());
                    }
                }
                Some(FileResponse::File(File { path, headers }))
            }
            path => path,
        }
    }
}

#[launch]
fn launch() -> _ {
    build().mount(
        "/",
        FileServer::empty()
            .filter_file(filter_dotfiles)
            .map_file(dir_root("static"))
            .map_file(normalize_dirs)
            .map_file(index("index.txt"))
            .and_rewrite(CachedCompression::new()),
    )
}
