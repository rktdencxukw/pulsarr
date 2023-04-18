mvn -Dmaven.test.skip=true install
rsync -avzPr pulsar-standalone/target/dependency/ ali-hk-1:~/pulsarr/dependency/ && rsync -avzPr pulsar-standalone/target/*.jar ali-hk-1:~/pulsarr/ && rsync -avzPr pulsar-standalone/target/dependency/ tc-gz-1:~/pulsarr/dependency/ && rsync -avzPr pulsar-standalone/target/*.jar tc-gz-1:~/pulsarr/ && rsync -avzPr pulsar-standalone/target/dependency/ hw-gz-975:~/pulsarr/dependency/ && rsync -avzPr pulsar-standalone/target/*.jar hw-gz-975:~/pulsarr/ && say 'Deploy done'

if [[ $? != 0 ]]; then
	say 'failed'
fi
