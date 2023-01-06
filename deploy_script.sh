#!/bin/bash 

# tag and push linklives-transcribed-indexer image
docker tag linklives-transcribed-indexer 282251075226.dkr.ecr.eu-west-1.amazonaws.com/linklives-transcribed-indexer:{image-tag}
docker push 282251075226.dkr.ecr.eu-west-1.amazonaws.com/linklives-transcribed-indexer:{image-tag}

# tag and push linklives-lifecourse-indexer image
docker tag linklives-lifecourse-indexer 282251075226.dkr.ecr.eu-west-1.amazonaws.com/linklives-lifecourse-indexer:{image-tag}
docker push 282251075226.dkr.ecr.eu-west-1.amazonaws.com/linklives-lifecourse-indexer:{image-tag}
