# Redis YCSB 1.0.0
This package is a YCSB implementation to test Redis. It is based on YCSB core 0.18.0.

## Requirements
- Java 17 JRE or JDK
- Linux, macOS, or Windows load generator system (8 cores / 32GB RAM minimum recommended)
- Redis Open Source or Redis Enterprise

## Quickstart

### 2. Set up YCSB
Download the distribution to begin testing.
```
curl -OLs https://github.com/mminichino/ycsb-redis/releases/download/v1.0.0/ycsb-redis-1.0.0.zip
```
```
unzip ycsb-redis-1.0.0.zip
```
```
cd ycsb-redis-1.0.0
```
Edit the `conf/db.properties` properties file to configure database settings.<br>
On Linux/macOS:
```
vi conf/db.properties
```
On Windows:
```
notepad conf\db.properties
```
If you are using Redis Enterprise, you can use the database creation tool to create a test database:<br>
```
bin/setup
```
To run all test scenarios:<br>
On Linux/macOS:
```
bin/ycsb-redis
```
On Windows:
```
bin\ycsb-redis.bat
```
