# Redis YCSB 2.0.7
This package is a YCSB implementation to test Redis. It is based on YCSB core 0.18.0.

## Requirements
- Java 11 or higher
- Linux, macOS, or Windows load generator system (8 cores / 32GB RAM minimum recommended)
- Redis Open Source or Redis Enterprise

## Quickstart

### 2. Set up YCSB
Download the distribution to begin testing.
```
unzip ycsb-redis.zip
```
```
cd ycsb-redis
```
On Linux/macOS:
```
vi conf/db.properties
```
On Windows:
```
notepad conf\db.properties
```
On Linux/macOS:
```
bin/ycsb-redis
```
On Windows:
```
bin\ycsb-redis.bat
```
