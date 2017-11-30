# Firehose analytics

[![GoDoc](https://godoc.org/github.com/matthewmueller/firehose-analytics?status.svg)](https://godoc.org/github.com/matthewmueller/firehose-analytics)
![](https://img.shields.io/badge/license-MIT-blue.svg)
![](https://img.shields.io/badge/status-stable-green.svg)

Send logs to AWS Firehose. This library first writes the metrics to disk, which you can periodically flush to AWS Firehose. 

This was built for instrumenting CLI tools, but could be used for anything where you have access to the local filesystem.

## Credits

Most of this code was pulled from: https://github.com/tj/go-cli-analytics. 

The main differences are that we're sending to Firehose instead of Segment and we're not storing the metrics in the user's home directory.

## License

MIT