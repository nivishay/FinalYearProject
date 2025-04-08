
# this script wont run on mysql ver 8
sudo docker run --rm -it dimitri/pgloader:latest \
pgloader \
mysql://stam:loaderpass@192.168.1.207:3306/sakila \
postgresql://postgres:postgres@192.168.1.207:5432/sakila

