#!/bin/bash
git pull
curr_cm_id=`git show-ref --heads`
last_cm_id=`cat .last_cm_id.txt`
cm_msg=`git log -1 --abbrev-commit`
if [ "$last_cm_id" == "$curr_cm_id" ]; then
        echo "Last commit doesn't change."
else
        echo "Last commit changed. Start building..." \
        && docker build . -t ubercadence/server:master --build-arg git_branch=master \
        && docker push ubercadence/server:master \
        && docker build . -f Dockerfile-cli -t ubercadence/cli:master --build-arg git_branch=master \
        && docker push ubercadence/cli:master \
        && mail -s "Docker image auto build succeeded HEAD: $curr_cm_id tag: master" longer@uber.com,maxim@uber.com,yiminc@uber.com,venkat@uber.com,krupapc@uber.com,samar@uber.com,wenquanx@uber.com,boweixu@uber.com,meiliang@uber.com,arthurg@uber.com,nathanbl@uber.com <<< "commit info: $cm_msg  " && echo $curr_cm_id > .last_cm_id.txt && echo "build succ!"
fi
