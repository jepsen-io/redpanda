# jepsen.redpanda

Tests for the Redpanda distributed queue.

## Installation

## Usage

`lein run test`

## FAQ

If you're running in containers Redpanda may fail to start, citing a need to
increase `/proc/sys/fs/aio-max-nr` on the host OS--individual containers can't
alter it themselves. Try

```
sudo sh -c 'echo 10000000 > /proc/sys/fs/aio-max-nr'
```

## License

Copyright © 2021 Jepsen. LLC

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.
