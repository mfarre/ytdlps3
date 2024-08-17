
# Youtube 2 S3

### Make your own Docker container

* `git clone git@github.com:mfarre/ytdlps3.git`
* `docker build --no-cache -t youtube-s3-uploader .`

### Run your container
`docker run --rm -e AWS_ACCESS_KEY_ID=<key> -e AWS_SECRET_ACCESS_KEY=<secret> -e AWS_DEFAULT_REGION=us-west-2 youtube-s3-uploader <videoId> <s3bucket> <folder/path in bucket>`


### Build on a mac run on something else:
`docker buildx create --use`
`docker buildx build --platform linux/amd64 -t youtube-s3-uploader:latest --push .`