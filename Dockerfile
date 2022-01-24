FROM conanio/gcc9

# use root
USER root

# setup conan
RUN pip install conan --upgrade &&\
    conan profile new default --detect &&\
    conan profile update settings.compiler.libcxx=libstdc++11 default

# copy data
COPY include /LKRS/include
COPY scripts /LKRS/scripts
COPY src /LKRS/src
COPY test /LKRS/test
COPY CMakeLists.txt /LKRS/CMakeLists.txt
COPY conanfile.txt /LKRS/conanfile.txt
RUN mkdir -p /LKRS/bin &&\
    mkdir -p /LKRS/build

#build
WORKDIR /LKRS/build
#RUN conan install .. -s build_type=Release --build
RUN cmake .. -G "Unix Makefiles" -DCMAKE_BUILD_TYPE=Release &&\
    cmake --build .

# build
#WORKDIR /LKRS/scripts
#RUN chmod +x ./build.sh
#RUN ./build.sh

# test
WORKDIR /LKRS
RUN ./bin/unitTests
