#!bin/bash

FILE_NAME=$1
text="Ini sample shell script. Namanya $FILE_NAME"

echo $text > $FILE_NAME
echo "File '$FILE_NAME' dibuat dengan sample text."