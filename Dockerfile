FROM hseeberger/scala-sbt:8u222_1.3.5_2.13.1
RUN mkdir -p /home/cs441project/app
COPY . /home/cs441project/app
WORKDIR /home/cs441project/app

CMD sbt "runMain com.server.HttpServer"
