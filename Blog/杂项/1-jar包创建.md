打包流程
1. 打开菜单栏File-Project Structure

     ![image-20210715175323735](D:\Tutorial\xiaomi\jar包的创建\image\image-20210715175323735.png)

2. 点击Artifacts

    ![image-20210715175341071](D:\Tutorial\xiaomi\jar包的创建\image\image-20210715175341071.png)

3. 点击 "+" - JAR - From module with depenencies

    ![image-20210715175358463](D:\Tutorial\xiaomi\jar包的创建\image\image-20210715175358463.png)

4. 后弹出如下界面，自此开始，各种问题就来了

   

首先Module中，我SocketDemo的Module含有SocketDemo、SocketDemo_main、SocketDemo_test三个，一定要选择main, **MF目录默认的就行**
        ![image-20210715175550288](D:\Tutorial\xiaomi\jar包的创建\image\image-20210715175550288.png)
        

点击OK
5. 点击Build-Artifacts

    ![image-20210715175730393](D:\Tutorial\xiaomi\jar包的创建\image\image-20210715175730393.png)

6. 点击Build

    

7. 此时Output directory便出现了jar包

   

8. 打开CMD窗口，运行jar包。一定要cd到jar包所在目录(Output directory)，一定要使用java - jar jar包名称

   ![image-20210715175754872](D:\Tutorial\xiaomi\jar包的创建\image\image-20210715175754872.png)
