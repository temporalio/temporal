CREATE USER 'temporal'@'%' IDENTIFIED BY 'temporal';
GRANT ALL PRIVILEGES ON *.* TO 'temporal'@'%';
FLUSH PRIVILEGES;