package com.example.utils;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.generator.AutoGenerator;
import com.baomidou.mybatisplus.generator.config.DataSourceConfig;
import com.baomidou.mybatisplus.generator.config.GlobalConfig;
import com.baomidou.mybatisplus.generator.config.PackageConfig;
import com.baomidou.mybatisplus.generator.config.StrategyConfig;
import com.baomidou.mybatisplus.generator.config.rules.DateType;
import com.baomidou.mybatisplus.generator.config.rules.NamingStrategy;

/**
 * @author xyh
 * @date 2021/1/21 21:36
 */
public class GenneratorCode {

    public static void main(String[] args) {
        // 1.创建代码生成器
        AutoGenerator mpg = new AutoGenerator();

        // 2.全局配置
        GlobalConfig gc = new GlobalConfig();
//        String projectPath = System.getProperty("user.dir");
        String projectPath = "D:\\kafka\\LV77_mmgrservice";
        gc.setOutputDir(projectPath + "/src/main/java");
        gc.setAuthor("IT05281");
        //生成后是否打开资源管理器
        gc.setOpen(false);
        //是否覆盖已有文件
        gc.setFileOverride(false);
        gc.setIdType(IdType.ID_WORKER_STR);
        gc.setDateType(DateType.ONLY_DATE);
        mpg.setGlobalConfig(gc);

        // 3.数据源配置
        DataSourceConfig dsc = new DataSourceConfig();
        dsc.setUrl("jdbc:mysql://55.14.61.43:3306/adsetllocal?characterEncoding=UTF-8");
        dsc.setDriverName("com.mysql.jdbc.Driver");
        dsc.setUsername("adsdb");
        dsc.setPassword("ADSdb123%");
        mpg.setDataSource(dsc);

        // 4.包配置
        PackageConfig pc = new PackageConfig();
        pc.setParent("com.cmb.zcgl");
        pc.setModuleName("mmgrservice");
        //生成的配置文件包路径
        pc.setController("controller");
        pc.setEntity("entity.po");
        pc.setService("service");
        pc.setMapper("mapper");
        mpg.setPackageInfo(pc);

        // 5. 策略配置
//        TemplateConfig templateConfig = new TemplateConfig();
        StrategyConfig strategy = new StrategyConfig();
        // 数据库中表的名字，表示要对哪些表进行自动生成controller,service,mapper....
//        strategy.setInclude("kafka_user","kafka_topic","kafka_topic_acl","kafka_user_apply","kafka_topic_apply","kafka_topic_acl");
        strategy.setInclude("kafka_group_acl","kafka_group_acl_apply");
        // 数据库表映射到实体的命名策略，驼峰命名法
        strategy.setNaming(NamingStrategy.underline_to_camel);
        // 生成实体时去掉表前缀
//        strategy.setTablePrefix("diff_");

        // 数据库表字段映射到实体的命名策略
        strategy.setColumnNaming(NamingStrategy.underline_to_camel);
        strategy.setRestControllerStyle(true); //restful api风格控制器
        strategy.setControllerMappingHyphenStyle(true); //url中驼峰转连字符

        mpg.setStrategy(strategy);

        // 6.执行
        mpg.execute();
    }
}
