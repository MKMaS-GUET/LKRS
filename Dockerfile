FROM conanio/gcc9
MAINTAINER Inno Fang

# use root
USER root

# setup conan
RUN pip install conan --upgrade &&\
    conan profile new default --detect &&\
    conan profile update settings.compiler.libcxx=libstdc++11 default

# copy data
COPY include /pisano/include
COPY scripts /pisano/scripts
COPY src /pisano/src
COPY test /pisano/test
COPY CMakeLists.txt /pisano/CMakeLists.txt
COPY conanfile.txt /pisano/conanfile.txt
RUN mkdir -p /pisano/bin &&\
    mkdir -p /pisano/build

#build
WORKDIR /pisano/build
#RUN conan install .. -s build_type=Release --build
RUN cmake .. -G "Unix Makefiles" -DCMAKE_BUILD_TYPE=Release &&\
    cmake --build .

# build
#WORKDIR /pisano/scripts
#RUN chmod +x ./build.sh
#RUN ./build.sh

# test
WORKDIR /pisano
RUN ./bin/unitTests
