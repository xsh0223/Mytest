extern crate reqwest;
extern crate url_scraper;
extern crate crossbeam_channel;
extern crate chrono;
#[macro_use] extern crate diesel;
#[macro_use] extern crate dotenv;


#[macro_use] extern crate bitflags;
use url_scraper::UrlScraper;
use reqwest::{Client, Url, header};
use crossbeam_channel as channel;
use channel::Receiver;
use std::sync::Arc;
use std::thread;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;
use chrono::{DateTime,FixedOffset};
use diesel::pg::PgConnection;
use diesel::prelude::*;
use dotenv::dotenv;
use std::env;

bitflags! {
    /// Flags for controlling the behavior of the crawler.
    pub struct Flags: u8 {
        /// Enable crawling across domains.
        const CROSS_DOMAIN = 1;
        /// Enable crawling outside of the specified directory.
        const CROSS_DIR = 2;
    }
}

pub mod schema;
use schema::*;
pub mod models;
use models::*;
mod scraper;
use scraper::*;

#[derive(Debug, Clone, PartialEq)]
enum CrawlSource {
    Single(String),
    Multiple(Vec<String>),
}

impl From<String> for CrawlSource {
    fn from(str:String) -> CrawlSource {
        CrawlSource::Single(str)
    }
}
pub type ErrorsCallback = Arc<Fn(fmt::Error) -> bool + Sync +Send>;
pub type PrefetchCallback = Arc<Fn(&Url) -> bool + Sync + Send>;
pub type PostfetchCallback = Arc<Fn(&Url,&header::HeaderMap) -> bool + Send + Sync>;

pub struct Crawler {
    urls: CrawlSource,
    threads: usize,
    flags: Flags,
    errors: ErrorsCallback,
    pre_fetch: PrefetchCallback,
    post_fetch: PostfetchCallback,
}

impl Crawler {
    pub fn new(source: impl Into<CrawlSource>) -> Self {
        Crawler {
            urls: source.into(),
            threads: 4,
            flags: Flags::empty(),
            errors: Arc::new(|_| true),
            pre_fetch: Arc::new(|_| true),
            post_fetch: Arc::new(|_,_| true),
        }
    }
    pub fn pre_fetch(mut self, pre_fetch: PrefetchCallback) -> Self{
        self.pre_fetch = pre_fetch;
        self
    }

    pub fn threads(mut self, n:usize) -> Self{
        self.threads = n;
        self
    }

    pub fn crawl(self) -> CrawlIter {
        let threads = self.threads;
        let client = Client::new();
        let flags = self.flags;

        let (scraper_tx, scraper_rx) = channel::unbounded::<String>();
        let (fetcher_tx, fetcher_rx) = channel::bounded::<Url>(threads * 4);
        let (output_tx, output_rx) = channel::bounded::<UrlEntry>(threads * 4);
        let kill = Arc::new(AtomicBool::new(false));
        let state = Arc::new(AtomicUsize::new(0));
        match self.urls {
            CrawlSource::Single(url) => scraper_tx.send(url),
            CrawlSource::Multiple(urls) => for url in urls {
                scraper_tx.send(url);
            }
        }
        {
            let state_ = state.clone();
            let client_ = client.clone();
            let scraper_rx_ = scraper_rx.clone();
            let fetcher_tx_ = fetcher_tx.clone();
            thread::spawn(move || {
                let mut Visited = Vec::new();
                let job_complete = || {
                    state_.load(Ordering::SeqCst) == threads
                        && scraper_rx_.is_empty()
                        && fetcher_tx_.is_empty()
                };

                loop {
                    let url_: String = match scraper_rx_.try_recv() {
                        Some(url) => url,
                        None => {
                            if job_complete() { break; }
                            thread::sleep(Duration::from_millis(1));
                            continue;
                        }
                    };

                    match UrlScraper::new_with_client(&url_, &client_) {
                        Ok(scraper) => for url in Scraper::new(scraper.into_iter(), &url_, &mut Visited, flags) {
                            fetcher_tx_.send(url);
                        }
                        Err(e) => {
                            break;
                        }
                    }
                }
            });
        }
        let pre_fetch = self.pre_fetch;
        //let temp = self.urls;
        for _ in 0..threads {
            let mut status = state.clone();
            let fetcher = fetcher_rx.clone();
            let scraper_tx = scraper_tx.clone();
            let client = client.clone();
            let temp = flags.clone();
            let output_tx = output_tx.clone();
            let pre_fetch = pre_fetch.clone();
            thread::spawn(move|| {
                status.fetch_add(1,Ordering::SeqCst);
                for url in fetcher {
                    status.fetch_sub(1,Ordering::SeqCst);

                    if !pre_fetch(&url) {
                        status.fetch_add(1,Ordering::SeqCst);
                        continue;
                    }

                    let head = match client.head(url.clone()).send() {
                        Ok(head) => head,
                        Err(e) => {
                            status.fetch_add(1,Ordering::SeqCst);
                            continue;
                        }
                    };

                    let headers = head.headers();
/*
                    if !post_fetch(&url,head.headers()) {
                        continue;
                    }
*/
                    if let Some(content_type) = headers.get(header::CONTENT_TYPE).and_then(|c| c.to_str().ok()) {
                        if content_type.starts_with("text/html") {
                            scraper_tx.send(url.to_string());
                            output_tx.send(UrlEntry::Html {url});
                        }
                        else {
                            println!("{}",content_type);
                            let length:u64 = headers.get(header::CONTENT_LENGTH)
                                .and_then(|c| c.to_str().ok())
                                .and_then(|c| c.parse().ok())
                                .unwrap_or(0);

                            let modified = headers.get(header::LAST_MODIFIED)
                                .and_then(|c| c.to_str().ok())
                                .and_then(|c| DateTime::parse_from_rfc2822(c).ok());

                            //output_tx.send(UrlEntry::File {url, length, modified, content_type:content_type.into()});
                        }
                    }

                    status.fetch_add(1,Ordering::SeqCst);
                }
            });
        };

        CrawlIter {
            recv: output_rx,
            kill,
        }
    }
}

pub struct CrawlIter {
    recv: channel::Receiver<UrlEntry>,
    kill: Arc<AtomicBool>,
}

impl Iterator for CrawlIter {
    type Item = UrlEntry;

    fn next(&mut self) -> Option<Self::Item> {
        self.recv.next()
    }
}

#[derive(Debug)]
pub enum UrlEntry {
    temp(String),
    Html { url: Url },
    File { url:Url, content_type:String, length:u64, modified:Option<DateTime<FixedOffset>> }
}


fn apt_filter(url: &Url) -> bool {
        let url = url.as_str();
        url.ends_with("/")
}

fn connect_db() -> PgConnection {
    dotenv().ok();

    let database_url = env::var("DATABASE_URL").expect("not a database");
    PgConnection::establish(&database_url).unwrap()
}

fn create_record(conn: &PgConnection, url: &str) -> usize{
    use schema::record;

    let record = NewRecord {
        url: url,
    };

    diesel::insert_into(record::table)
        .values(&record)
        .execute(conn)
        .unwrap()

}

#[derive(Debug)]
pub enum Error {
    Scraper { why: url_scraper::Error },
    Request { why: reqwest::Error }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "error while {}", match *self {
            Error::Scraper { ref why } => format!("scraping a page: {}", why),
            Error::Request { ref why } => format!("requesting content: {}", why),
        })
    }
}


impl From<url_scraper::Error> for Error {
    fn from(why: url_scraper::Error) -> Error {
        Error::Scraper { why }
    }
}

impl From<reqwest::Error> for Error {
    fn from(why: reqwest::Error) -> Error {
        Error::Request { why }
    }
}

pub fn main() {
        // Create a crawler designed to crawl the given website.
        let err:reqwest::Error;

        let crawler = Crawler::new("http://apt.pop-os.org/".to_owned())
            // Use four threads for fetching
            .threads(4)
            // Check if a URL matches this filter before performing a HEAD request on it.
            .pre_fetch(Arc::new(apt_filter))
            // Initialize the crawler and begin crawling. This returns immediately.
            .crawl();
        let conn = connect_db();
        // Process url entries as they become available

        for file in crawler {
            match file {
                UrlEntry::Html{url:url} => create_record(&conn, &url.to_string()),
                UrlEntry::temp(a) => 1,
                _ => 1,
            };
        }
}



