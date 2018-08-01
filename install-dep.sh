if ! type brew &> /dev/null; then
    curl https://raw.githubusercontent.com/golang/dep/master/install.sh | sh
else 
    brew install dep 
fi 
