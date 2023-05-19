image-base:
	docker build -f Dockerfile.base -t bunny_order:base .

image:
	docker-compose build --no-cache

up:
	docker-compose up -d

down:
	docker-compose down
