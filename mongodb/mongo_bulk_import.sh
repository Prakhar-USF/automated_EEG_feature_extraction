# To run this example on the command line:
# bash mongo_bulk.import.sh [s3 root path] [dbName] [collectionName] [mongos port]
# collectionName is one of the following:
# eeg_metadata, eeg_features, tracking, eeg_raw


OIFS="$IFS"
IFS=$'\n'
fileList=$(aws s3 ls s3://$1 --recursive | awk '{$1=$2=$3=""; print $0}' | sed 's/^[ \t]*//')

for f in $fileList; do
    #echo $f;
    echo "Processing $f file...";
    aws s3 cp s3://msds694-usfcaeeg/$f - | mongoimport -d $2 -c $3 --port $4;
done
IFS="$OIFS"