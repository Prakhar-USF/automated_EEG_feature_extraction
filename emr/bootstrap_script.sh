#!/bin/bash -xe

# Non-standard and non-Amazon Machine Image Python modules:
# Fix for another AWS bug --> https://forums.aws.amazon.com/thread.jspa?messageID=878824&tstart=0
sudo cp /etc/spark/conf/log4j.properties.template /etc/spark/conf/log4j.properties
sudo sed -i 's/log4j.rootCategory=INFO, console/log4j.rootCategory=ERROR,console/' /etc/spark/conf/log4j.properties
