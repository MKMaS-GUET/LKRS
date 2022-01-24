# LKRS

A scalable lightweight RDF knowledge retrieval system

![](https://img.shields.io/badge/Conan-1.42-brightgreen)

## build

Make sure you have installed [conan](https://conan.io/), and can use it correctly to build with [CMake](https://cmake.org/)

If you're not familiar with conan, you can install it by pip, that is `pip install conan`, and have a [quick start](https://docs.conan.io/en/latest/getting_started.html)

### On Linux

build manually

```shell
mkdir build
cd build
cmake .. -G "Unix Makefiles"  -DCMAKE_BUILD_TYPE=Release 
cmake --build .
```

build with script

```shell
cd scripts
./build.sh
```

### On Windows

build manually

```shell
mkdir build
cd build
cmake .. 
cmake --build . --config Release
```

build with script

```shell
cd scripts
./build.bat
```

### Build with Docker 

```shell
docker build --tag lkrs:demo .
docker run -it --name lkrs lkrs:demo
```
