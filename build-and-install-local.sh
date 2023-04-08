#!/bin/sh
mvn -T 1.5C -Dmaven.test.skip=true -Pall-modules && rsync -avzPr /Users/kc/.m2/repository/ai/platon/pulsar/ kc@192.168.68.203:/Users/kc/.m2/repository/ai/platon/pulsar/ && say 'done'

if [[ $? != 0 ]];then
	say 'failed'
fi
