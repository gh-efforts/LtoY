# LtoY
Migrate boost index and deal from levelDB to yugabyteDB 
## build
```bash
#ffi
git submodule update --init --recursive
cd extern/filecoin-ffi
make
#LtoT
cd ../../
go build
```
## Usage
migrate index
```bash
./LtoY --boost-repo /media/nvme/boost --vv migrate --hosts 127.0.0.1 --connect-string "postgresql://yugabyte:yugabyte@127.0.0.1:5433/yugabyte?sslmode=disable" index
```
migrate deal
```bash
./LtoY --boost-repo /media/nvme/boost --vv migrate --hosts 127.0.0.1 --connect-string "postgresql://yugabyte:yugabyte@127.0.0.1:5433/yugabyte?sslmode=disable" deal
```
migrate check for pieceCID (baga6ea4seaqpzzc3lwrf25tntnff75dlib4te6fa437mnisglqtujma2ayzeaja)
```bash
./LtoY --boost-repo /media/nvme/boost --vv migrate --hosts 127.0.0.1 --connect-string "postgresql://yugabyte:yugabyte@127.0.0.1:5433/yugabyte?sslmode=disable" baga6ea4seaqpzzc3lwrf25tntnff75dlib4te6fa437mnisglqtujma2ayzeaja
```
