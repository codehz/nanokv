<h1>
<img src="https://github.com/user-attachments/assets/dd946ac1-2f41-4c95-a698-888124f70114" width="96px" />
NanoKV
</h1>

[![C++ CI](https://github.com/codehz/nanokv/actions/workflows/build.yml/badge.svg)](https://github.com/codehz/nanokv/actions/workflows/build.yml)
[![Docker CI](https://github.com/codehz/nanokv/actions/workflows/docker.yml/badge.svg)](https://github.com/codehz/nanokv/actions/workflows/docker.yml)
[![typedoc](https://github.com/codehz/nanokv/actions/workflows/typedoc.yml/badge.svg)](https://github.com/codehz/nanokv/actions/workflows/typedoc.yml)

A Minimal and persistent key-value database.

## Features

1. Basic operations of a KV database: Retrieve/Modify/Delete key-value pairs
2. Transaction model based on optimistic concurrency control
3. to deploy, only requires a single binary file or a Docker image for deployment (ghcr.io/codehz/nanokv)
4. No active restrictions on the length of keys (though excessively long content may lead to poor performance or even crash the process)
5. Persistent storage, not limited by memory constraints

## LICENSE

MIT