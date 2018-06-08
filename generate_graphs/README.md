# scala_boilerplate

a boilerplate for scala programs to be built and compiled with sbt. please note that this is configured to work with my scala setup, so this may not work for you - that being said, there are only a couple of edits to be made to get this running in your environment (file paths in build.sbt and simpleApp.scala)

## Installation

git clone "https://github.com/adityachatterjee42/scala_boilerplate"

## Usage

cd scala_boilerplate

sbt package

/opt/spark/bin/spark-submit --class "SimpleApp" --master local[4] target/scala-2.11/simple-project_2.11-1.0.jar
