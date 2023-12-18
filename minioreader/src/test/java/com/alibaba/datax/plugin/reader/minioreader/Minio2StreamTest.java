package com.alibaba.datax.plugin.reader.minioreader;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.Engine;
import com.alibaba.fastjson2.JSONObject;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.Random;

@Ignore
public class Minio2StreamTest {

    private static final String host = "192.168.56.105";
    private static final Random random = new Random(System.currentTimeMillis());

    @Test
    public void jobTest() throws Throwable {
        MinioReader.Job job = new MinioReader.Job();

        Configuration conf = Configuration.from(new File("src/test/resources/minio2streamer.json"));
        String reader = ((JSONObject) conf.get("job"))
                .getJSONArray("content")
                .getJSONObject(0)
                .getJSONObject("reader")
                .getString("parameter");
        job.setPluginJobConf(Configuration.from(reader));

        job.init();
        job.prepare();
        job.post();
        // 先整体编译打包：mvn -U clean package assembly:assembly -Dmaven.test.skip=true
        // when
//        String[] params = {"-mode", "standalone", "-jobid", "-1", "-job", "src/test/resources/minio2streamer.json"};
//        System.setProperty("datax.home", "../target/datax/datax");
//        Engine.entry(params);
    }

    @Test
    public void taskTest() throws Throwable {
        MinioReader.Task task = new MinioReader.Task();

        Configuration conf = Configuration.from(new File("src/test/resources/minio2streamer.json"));
        String reader = ((JSONObject) conf.get("job"))
                .getJSONArray("content")
                .getJSONObject(0)
                .getJSONObject("reader")
                .getString("parameter");
        task.setPluginJobConf(Configuration.from(reader));


//        task.init();
//        task.prepare();
//        task.startRead(null);

        // when
        String[] params = {"-mode", "standalone", "-jobid", "-1", "-job", "src/test/resources/minio2streamer.json"};
        System.setProperty("datax.home", "../target/datax/datax");
        Engine.entry(params);
    }





}
