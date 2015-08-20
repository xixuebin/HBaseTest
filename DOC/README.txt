1:使用说明：
开发工具：idea 12
项目管理：mvn

2:部署步骤：
拷贝libhadoop.so  libsnappy.so 到 java.library.path 中。  linux可以是/usr/lib

注意事项：libhadoop.so libsnappy.so 的版本信息，对应的操作系统信息



打成jar之后，如果运行提示找不到主方法，请手动修改MANIFEST.MF文件，添加执行入口


