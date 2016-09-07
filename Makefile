TAG = lshift.de/rammler

default:
	docker build -t $(TAG) .

run:
	docker run -d --name rammler --link rabbitmq $(TAG)

.PHONY: default run
