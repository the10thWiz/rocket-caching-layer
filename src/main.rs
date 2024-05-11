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
    fs::{FileServer, FileServerResponse, Index, NormalizeDirs, Options, Rewrite},
    http::Header,
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
                if coding.contains(';') {
                    if let Some((_, val)) = coding.split_once('=') {
                        let val: f32 = val.trim().parse().unwrap_or(0.);
                        if val != 0. {
                            Some(coding.split_once(';').unwrap().0.trim())
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

    fn dispatch(&self, algo: Algorithm, path: PathBuf, root: PathBuf) {
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

            let success =
                match Self::compress(compressor, &root.join(&path), &root.join(new_path)).await {
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

impl Rewrite for CachedCompression {
    fn rewrite(
        &self,
        req: &rocket::Request<'_>,
        path: rocket::fs::FileServerResponse,
        root: &std::path::Path,
    ) -> rocket::fs::FileServerResponse {
        match path {
            FileServerResponse::File {
                mut name,
                modified,
                mut headers,
            } => {
                if let Some(algo) = self.get_valid(req) {
                    if let Some(info) = self.map.get(&name) {
                        if info.compressions.contains(&algo) {
                            // TODO: this unwraps a bunch of errors, that should be handled
                            let new_name =
                                format!("{}.{algo}", name.file_name().unwrap().to_str().unwrap());
                            name.set_file_name(new_name);
                            headers.add(Header::new("Content-Encoding", algo.to_string()));
                        } else {
                            self.dispatch(algo, name.clone(), root.to_path_buf());
                        }
                    } else {
                        self.dispatch(algo, name.clone(), root.to_path_buf());
                    }
                }
                FileServerResponse::File {
                    name,
                    modified,
                    headers,
                }
            }
            path => path,
        }
    }
}

#[launch]
fn launch() -> _ {
    build().mount(
        "/",
        FileServer::new("static", Options::None)
            .rewrite(NormalizeDirs)
            .rewrite(Index("index.txt"))
            .rewrite(CachedCompression::new()),
    )
}
