#[deny(missing_docs)]
use std::{
    fmt::Display,
    io,
    path::{Path, PathBuf},
    sync::Arc,
};

use dashmap::DashMap;
use flate2::{Compress, Compression, Status};
use rocket::{
    fs::rewrite::{Rewrite, Rewriter},
    http::{ContentType, Header},
    tokio::io::{AsyncReadExt, AsyncWriteExt}, trace::error,
};

/// Supported compression algorithms
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Algorithm {
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

/// A rewriter for `FileServer`, that implements cached compression.
///
/// Implements caching by storing a compressed copy of the file to be served.
/// When a request is made for a file for the first time, a task is dispatched
/// to generate a compressed copy of the file, and future requests (after the
/// compression task has completed) will send the compressed version.
pub struct CachedCompression {
    map: Arc<DashMap<PathBuf, Info>>,
}

impl CachedCompression {
    /// Create a default caching compression rewrite. Should be added at or near
    /// the end of the chain.
    pub fn new() -> Self {
        Self {
            map: Arc::new(DashMap::new()),
        }
    }

    fn get_valid(&self, req: &rocket::Request<'_>) -> Option<Algorithm> {
        req.headers()
            .get("Accept-Encoding")
            .flat_map(|v| v.split(|c| c == ','))
            .filter_map(|coding| {
                let mut parts = coding.split(';');
                let name = parts.next()?;
                for (p, val) in parts.filter_map(|p| p.split_once('=')) {
                    let val: f32 = val.trim().parse().unwrap_or(0.);
                    if val == 0. && p.trim() == "q" {
                        return None;
                    }
                }
                Some(name.trim())
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
                Err(e) => {
                    error!(?e, "Error when compressing file {}", path.display());
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
    fn rewrite<'h>(
        &self,
        path: Option<Rewrite<'h>>,
        req: &rocket::Request<'_>,
    ) -> Option<Rewrite<'h>> {
        match path {
            Some(Rewrite::File(mut file)) => {
                if let Some(algo) = self.get_valid(req) {
                    if self
                        .map
                        .get(file.path.as_ref())
                        .is_some_and(|info| info.compressions.contains(&algo))
                    {
                        // Since we change the path, it seems like we override any
                        // automatic content-type detection, so we just do it manually
                        // We could implement this directly on File as well
                        if let Some(ct) = content_type_from_path(&file.path) {
                            file.headers.add(ct);
                        }
                        file.headers
                            .add(Header::new("Content-Encoding", algo.to_string()));
                        let new_name = format!(
                            "{}.{algo}",
                            file.path.file_name().and_then(|s| s.to_str()).unwrap_or("")
                        );
                        file.path.to_mut().set_file_name(new_name);
                    } else {
                        self.dispatch(algo, file.path.clone().into_owned());
                    }
                }
                Some(Rewrite::File(file))
            }
            path => path,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use rocket::{
        async_test, build,
        fs::{rewrite::DirIndex, FileServer},
        http::Status,
        local::asynchronous::Client,
        tokio::time::sleep,
        Build, Rocket,
    };

    use super::*;

    fn launch() -> Rocket<Build> {
        build().mount(
            "/",
            FileServer::without_index("static")
                .rewrite(DirIndex::unconditional("index.txt"))
                .rewrite(CachedCompression::new()),
        )
    }

    async fn gzipped_req(
        client: &mut Client,
        accept: impl Into<Option<&'static str>>,
        compressed: bool,
    ) {
        let res = client.get("/");
        let res = match accept.into() {
            Some(accept) => res.header(Header::new("Accept-Encoding", accept)),
            None => res,
        }
        .dispatch()
        .await;
        assert_eq!(res.status(), Status::Ok);
        assert_eq!(
            res.headers().get_one("Content-Type").unwrap(),
            "text/plain; charset=utf-8"
        );
        if compressed {
            assert_eq!(res.headers().get_one("Content-Encoding").unwrap(), "gzip");
            assert_eq!(
                res.into_bytes().await.unwrap(),
                include_bytes!("../static/index.txt.pre-gziped")
            );
        } else {
            assert_eq!(res.headers().get_one("Content-Encoding"), None);
            assert_eq!(
                res.into_string().await.unwrap(),
                include_str!("../static/index.txt")
            );
        }
    }

    #[async_test]
    async fn simple_test() {
        let mut client = Client::untracked(launch()).await.unwrap();
        gzipped_req(&mut client, "gzip", false).await;
        sleep(Duration::from_millis(400)).await;

        gzipped_req(&mut client, "gzip", true).await;
        gzipped_req(&mut client, "gzip; q=1", true).await;
        gzipped_req(&mut client, "gzip; q = 0.5", true).await;
        gzipped_req(&mut client, "gzip; q = 0", false).await;
        gzipped_req(&mut client, "flate", false).await;
        gzipped_req(&mut client, "flate,gzip", true).await;
        gzipped_req(&mut client, None, false).await;
    }
}
