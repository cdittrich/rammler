# rammler

rammler is an AMQP 0.9.1 proxy, the protocol implemented by recent
versions of the RabbitMQ message broker. Its main purpose is to
dispatch incoming client connections to different RabbitMQ servers
depending on the username the client tries to authenticate with. That
way, a topology of different servers can be used through a single
endpoint for clients.

## Installation

TBD.

## Usage

    $ rammler [options]

## Options

    -h, --help                           Display this help and exit
        --version                        Output version information and exit
    -c, --config FILE  /etc/rammler.edn  Location of configuration file

## License

Copyright © 2016 LShift Services GmbH

rammler is free software; you can redistribute it and/or modify it
under the terms of the Affero GNU Lesser General Public License as
published by the Free Software Foundation; either version 3 of the
License, or (at your option) any later version.

rammler is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this program.  If not, see
<http://www.gnu.org/licenses/>.
