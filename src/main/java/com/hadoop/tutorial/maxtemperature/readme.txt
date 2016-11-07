1.准备
    把包下input中的数据放在项目根目录下

2.在runConfig中配置如下,本地运行:
main class: org.apache.hadoop.util.RunJar
program arguments: /Users/baoyu/idea_workspace/hadoop_demo/out/artifacts/maxtemperature/maxtemperature.jar com.hadoop.maxtemperature.MaxTemperature input/ output/
working directory: /Users/baoyu/idea_workspace/hadoop_demo
use classpath of module: hadoop_demon