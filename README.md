# XP

[![tag](https://img.shields.io/github/tag/davidvella/xp.svg)](https://github.com/davidvella/xp/releases)
![Go Version](https://img.shields.io/badge/Go-%3E%3D%201.23-%23007d9c)
[![GoDoc](https://godoc.org/github.com/davidvella/xp?status.svg)](https://pkg.go.dev/github.com/davidvella/xp)
[![Go report](https://goreportcard.com/badge/github.com/davidvella/xp)](https://goreportcard.com/report/github.com/davidvella/xp)
[![codecov](https://codecov.io/gh/davidvella/xp/graph/badge.svg?token=RSRKFCP1A0)](https://codecov.io/gh/davidvella/xp)

> Because Windows XP was the best Microsoft Windows OS.

Windows are fundamental to processing infinite data streams. They partition the
stream into finite "buckets" that enable computational analysis. This library
implements basic windowing functionality for streams using NoSQL database
components, including a sequential record format (recordio), a sorted string
table (sstable), and a write-ahead log (wal). I developed this library to enable
stream windowing without requiring complex frameworks like Apache Beam or Apache Flink.

## Features

- Stream Data Management Using Windows: Efficiently processes and organizes
  streaming data through window-based segmentation
- Streamlined Implementation: Maintains simplicity in design and execution
- Core Library Dependencies: Functions exclusively with standard library
  components, eliminating external dependency requirements

## Installation

```bash
go install github.com/davidvella/xp
```

Usage
-----
A basic example:

```go
TBD
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file
for details.

## Why "XP"?

Named after Windows XP, arguably the most beloved Microsoft Windows operating
system ever released.

## Author

David Vella - [@davidvella](https://github.com/davidvella)

## Acknowledgments

- Inspired by the legendary Windows XP
- Use a go implementation of a [tournament
  tree](https://gist.github.com/bboreham/11f8a11b9723f85d2fb7c47dc4f48159)