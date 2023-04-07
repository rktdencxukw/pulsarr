#mvn -T 1.5C -Dmaven.test.skip=true
rsync -avzPr pulsar-standalone/target/dependency/ ali-hk-1:~/pulsarr/dependency/
rsync -avzPr pulsar-standalone/target/*.jar ali-hk-1:~/pulsarr/
rsync -avzPr pulsar-standalone/target/dependency/ tc-gz-1:~/pulsarr/dependency/
rsync -avzPr pulsar-standalone/target/*.jar tc-gz-1:~/pulsarr/

say 'Deploy done'
