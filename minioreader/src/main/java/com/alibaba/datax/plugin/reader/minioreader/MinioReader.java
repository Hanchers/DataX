package com.alibaba.datax.plugin.reader.minioreader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.reader.minioreader.util.MinioSplitUtil;
import com.alibaba.datax.plugin.unstructuredstorage.FileFormat;
import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredStorageReaderUtil;
import com.alibaba.datax.plugin.unstructuredstorage.reader.binaryFileUtil.BinaryFileReaderUtil;
import com.alibaba.datax.plugin.unstructuredstorage.reader.split.StartEndPair;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.TypeReference;
import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSSException;
import io.minio.BucketExistsArgs;
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.Result;
import io.minio.messages.Item;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class MinioReader extends Reader {
    private static final Logger READER_LOG = LoggerFactory.getLogger(MinioReader.class);

    /**
     * 初始化client
     * @param readerOriginConfig 配置
     * @return
     */
    private static MinioClient initClient(Configuration readerOriginConfig) {
        MinioClient minioClient = MinioClient.builder()
                .endpoint(readerOriginConfig.getString(Key.ENDPOINT))
                .credentials(readerOriginConfig.getString(Key.ACCESSID), readerOriginConfig.getString(Key.ACCESSKEY))
                .build();
        READER_LOG.debug("init MinioClient success {}", JSON.toJSONString(minioClient));
        return minioClient;
    }

    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory
                .getLogger(MinioReader.Job.class);

        private Configuration readerOriginConfig = null;

        private MinioClient minioClient = null;
        private String endpoint;
        private String accessId;
        private String accessKey;
        private String bucket;
        private boolean successOnNoObject;
        private Boolean isBinaryFile;

        private List<String> objects;
        private List<Pair<String, Long>> objectSizePairs; /*用于任务切分的依据*/

        private String fileFormat;

        @Override
        public void init() {
            LOG.debug("init() begin...");
            this.readerOriginConfig = this.getPluginJobConf();
            this.basicValidateParameter();
            this.fileFormat = this.readerOriginConfig.getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.FILE_FORMAT,
                    com.alibaba.datax.plugin.unstructuredstorage.reader.Constant.DEFAULT_FILE_FORMAT);

            this.isBinaryFile = FileFormat.getFileFormatByConfiguration(this.readerOriginConfig).isBinary();
            this.validate();
            UnstructuredStorageReaderUtil.validateCsvReaderConfig(this.readerOriginConfig);
            this.successOnNoObject = this.readerOriginConfig.getBool(
                    Key.SUCCESS_ON_NO_Object, false);
            LOG.debug("init() ok and end...");
        }


        private void basicValidateParameter(){
            endpoint = this.readerOriginConfig.getString(Key.ENDPOINT);
            if (StringUtils.isBlank(endpoint)) {
                throw DataXException.asDataXException(
                        MinioReaderErrorCode.CONFIG_INVALID_EXCEPTION,"invalid endpoint");
            }

            accessId = this.readerOriginConfig.getString(Key.ACCESSID);
            if (StringUtils.isBlank(accessId)) {
                throw DataXException.asDataXException(
                        MinioReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                        "invalid accessId");
            }

            accessKey = this.readerOriginConfig.getString(Key.ACCESSKEY);
            if (StringUtils.isBlank(accessKey)) {
                throw DataXException.asDataXException(
                        MinioReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                        "invalid accessKey");
            }
        }
        // warn: 提前验证endpoint,accessId,accessKey,bucket,object的有效性
        private void validate() {

            minioClient = initClient(this.readerOriginConfig);

            bucket = this.readerOriginConfig.getString(Key.BUCKET);
            try {
                if (StringUtils.isBlank(bucket)) {
                    throw DataXException.asDataXException(
                            MinioReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                            "invalid bucket");
                }else if(!minioClient.bucketExists(BucketExistsArgs.builder().bucket(bucket).build())){
                    throw DataXException.asDataXException(
                            MinioReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                            "invalid bucket");
                }
            } catch (DataXException dxe) {
                throw dxe;
            } catch (Exception e) {
                LOG.error("valid bucket error", e);
                throw DataXException.asDataXException(
                        MinioReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                        "valid bucket error");
            }


            String object = this.readerOriginConfig.getString(Key.OBJECT);
            if (StringUtils.isBlank(object)) {
                throw DataXException.asDataXException(
                        MinioReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                        "invalid object");
            }

            if (this.isBinaryFile){
                return;
            }
            UnstructuredStorageReaderUtil.validateParameter(this.readerOriginConfig);
        }



        @Override
        public void prepare() {
            // 将每个单独的 object 作为一个 slice
            this.objectSizePairs = parseOriginObjectSizePairs(readerOriginConfig.getList(Key.OBJECT, String.class));
            this.objects = parseOriginObjects(readerOriginConfig.getList(Key.OBJECT, String.class));
            UnstructuredStorageReaderUtil.setSourceFileName(readerOriginConfig, this.objects);
            UnstructuredStorageReaderUtil.setSourceFile(readerOriginConfig, this.objects);
        }

        @Override
        public void post() {

            LOG.debug("post()");
        }

        @Override
        public void destroy() {
            LOG.debug("destroy()");
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            LOG.debug("split() begin...");

            List<Configuration> readerSplitConfigs;

            if (objects.isEmpty() && this.successOnNoObject) {
                readerSplitConfigs = new ArrayList<>();
                Configuration splitedConfig = this.readerOriginConfig.clone();
                splitedConfig.set(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.SPLIT_SLICE_CONFIG, null);
                readerSplitConfigs.add(splitedConfig);
                LOG.info("no Minio object to be read");
                LOG.debug("split() ok and end...");
                return readerSplitConfigs;
            }else if (objects.isEmpty()) {
                throw DataXException.asDataXException(
                        MinioReaderErrorCode.EMPTY_BUCKET_EXCEPTION,
                        String.format("Unable to find the object to read. Please confirm your configured item [bucket]: %s object: %s",
                                this.readerOriginConfig.get(Key.BUCKET),
                                this.readerOriginConfig.get(Key.OBJECT)));
            }

            /**
             * 当文件类型是text纯文本文件,并且不是压缩的情况下,
             * 可以对纯文本文件进行内部切分实现并发读取, 如果用户不希望对文件拆分, 可以指定fileFormat为csv
             *
             * 注意：这里判断文件是否为text以及是否压缩，信息都是通过任务配置项来获取的
             *
             * 这里抽出一个方法来判断是否需要分片
             * */
            MinioSplitUtil ossFileSplit = new MinioSplitUtil(this.minioClient, this.bucket);
            long t1 = System.currentTimeMillis();
            readerSplitConfigs = ossFileSplit.getSplitedConfigurations(this.readerOriginConfig, this.objectSizePairs,
                    adviceNumber);
            long t2 = System.currentTimeMillis();
            LOG.info("all split done, cost {}ms", t2 - t1);
            /**
             * 在日志中告知用户,为什么实际datax切分跑的channel数会小于用户配置的channel数
             * 注意：这里的报告的原因不准确，报的原因是一个文件一个task，所以最终切分数小于用户配置数，实际原因还有单文件切分时，
             * 单文件的大小太小（理论64M一个block），导致问题比较少
             */
            if(readerSplitConfigs.size() < adviceNumber){
                LOG.info("[Note]: During OSSReader data synchronization, one file can only be synchronized in one task. You want to synchronize {} files " +
                                "and the number is less than the number of channels you configured: {}. " +
                                "Therefore, please take note that DataX will actually have {} sub-tasks, that is, the actual concurrent channels = {}",
                        objects.size(), adviceNumber, objects.size(), objects.size());
            }
            LOG.info("split() ok and end...");
            return readerSplitConfigs;
        }

        private List<String> parseOriginObjects(List<String> originObjects) {
            List<String> objList = new ArrayList<>();

            if (this.objectSizePairs == null) {
                this.objectSizePairs = parseOriginObjectSizePairs(originObjects);
            }

            for (Pair<String, Long> objSizePair : this.objectSizePairs) {
                objList.add(objSizePair.getKey());
            }

            return objList;
        }

        private List<Pair<String, Long>> parseOriginObjectSizePairs(List<String> originObjects) {
            List<Pair<String, Long>> parsedObjectSizePaires = new ArrayList<>();

            for (String object : originObjects) {
                int firstMetaChar = (object.indexOf('*') > object.indexOf('?')) ? object
                        .indexOf('*') : object.indexOf('?');

                if (firstMetaChar != -1) {
                    int lastDirSeparator = object.lastIndexOf(
                            IOUtils.DIR_SEPARATOR, firstMetaChar);
                    String parentDir = object
                            .substring(0, lastDirSeparator + 1);
                    List<Pair<String, Long>> allRemoteObjectSizePairs = getAllRemoteObjectsKeyAndSizeInDir(parentDir);
                    Pattern pattern = Pattern.compile(object.replace("*", ".*")
                            .replace("?", ".?"));

                    for (Pair<String, Long> remoteObjectSizePair : allRemoteObjectSizePairs) {
                        if (pattern.matcher(remoteObjectSizePair.getKey()).matches()) {
                            parsedObjectSizePaires.add(remoteObjectSizePair);
                            LOG.info(String
                                    .format("add object [%s] as a candidate to be read.",
                                            remoteObjectSizePair.getKey()));
                        }
                    }
                } else {
                    // 如果没有配正则匹配,那么需要对用户自己配置的object存在性进行检测
                    try{
//                        GetObjectArgs args = GetObjectArgs.builder().bucket(bucket).object(object).build();
//                        minioClient.getObject(args);
//                        ObjectMetadata objMeta = minioClient.getObjectMetadata(bucket, object);
//                        parsedObjectSizePaires.add(new MutablePair<>(object, objMeta.getContentLength() <= MinioSplitUtil.SINGLE_FILE_SPLIT_THRESHOLD_IN_SIZE ? -1L : objMeta.getContentLength()));
                        parsedObjectSizePaires.add(new MutablePair<>(object,  -1L ));
                        LOG.info(String.format(
                                "add object [%s] as a candidate to be read.",
                                object));
                    }catch (Exception e){
                        trackOssDetailException(e, object);
                    }
                }
            }
            return parsedObjectSizePaires;
        }

        // 对oss配置异常信息进行细分定位
        private void trackOssDetailException(Exception e, String object){
            // 对异常信息进行细分定位
            String errorMessage = e.getMessage();
            if(StringUtils.isNotBlank(errorMessage)){
                if(errorMessage.contains("UnknownHost")){
                    // endPoint配置错误
                    throw DataXException.asDataXException(
                            MinioReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                            "The endpoint you configured is not correct. Please check the endpoint configuration", e);
                }else if(errorMessage.contains("InvalidAccessKeyId")){
                    // accessId配置错误
                    throw DataXException.asDataXException(
                            MinioReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                            "The accessId you configured is not correct. Please check the accessId configuration", e);
                }else if(errorMessage.contains("SignatureDoesNotMatch")){
                    // accessKey配置错误
                    throw DataXException.asDataXException(
                            MinioReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                           "The accessKey you configured is not correct. Please check the accessId configuration", e);
                }else if(errorMessage.contains("NoSuchKey")){
                    if (e instanceof OSSException) {
                        OSSException ossException = (OSSException) e;
                        if ("NoSuchKey".equalsIgnoreCase(ossException
                                .getErrorCode()) && this.successOnNoObject) {
                            LOG.warn(String.format("oss file %s is not exits to read:", object), e);
                            return;
                        }
                    }
                    // object配置错误
                    throw DataXException.asDataXException(
                            MinioReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                            "The object you configured is not correct. Please check the accessId configuration");
                }else{
                    // 其他错误
                    throw DataXException.asDataXException(
                            MinioReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                            String.format("Please check whether the configuration of [endpoint], [accessId], [accessKey], [bucket], and [object] are correct. Error reason: %s",e.getMessage()), e);
                }
            }else{
                throw DataXException.asDataXException(
                        MinioReaderErrorCode.CONFIG_INVALID_EXCEPTION,
                        "The configured json is invalid", e);
            }
        }

        private List<Pair<String, Long>> getAllRemoteObjectsKeyAndSizeInDir(String parentDir)
                throws OSSException, ClientException{
            List<Pair<String, Long>> objectSizePairs = new ArrayList<>();
            List<Item> objectListings  = getRemoteObjectListings(parentDir);

            if (objectListings.isEmpty()) {
                return objectSizePairs;
            }

            for (Item item : objectListings){
                Pair<String, Long> objNameSize = new MutablePair<>(item.objectName(), item.size() <= MinioSplitUtil.SINGLE_FILE_SPLIT_THRESHOLD_IN_SIZE ? -1L : item.size());
                objectSizePairs.add(objNameSize);

            }

            return  objectSizePairs;
        }

        private List<Item> getRemoteObjectListings(String parentDir) throws OSSException, ClientException {

            List<Item> remoteObjectListings = new ArrayList<>();

            LOG.debug("Parent folder: {}", parentDir);
            List<String> remoteObjects = new ArrayList<>();
            MinioClient client = initClient(this.readerOriginConfig);

            try {
                String bucket = readerOriginConfig.getString(Key.BUCKET);

                ListObjectsArgs objectsArgs = ListObjectsArgs.builder().bucket(bucket).prefix(parentDir).build();
                Iterable<Result<Item>> iterable = client.listObjects(objectsArgs);
                for (Result<Item> itemResult : iterable) {
                    Item remoteObject = null;
                    try {
                        remoteObject = itemResult.get();
                    } catch (Exception ee) {
                        LOG.error("ListObjects error", ee);
                    }

                    if (null != remoteObject) {
                        LOG.info("ListObjects name: {} versionId: {}", remoteObject.objectName(), remoteObject.versionId());
                    } else {
                        LOG.info("ListObjectsRequest get null");
                    }
                    remoteObjectListings.add(remoteObject);
                }
            } catch (Exception e) {
                trackOssDetailException(e, null);
            }

            return remoteObjectListings;
        }
    }

    public static class Task extends Reader.Task {
        private static Logger LOG = LoggerFactory.getLogger(Reader.Task.class);

        private Configuration readerSliceConfig;
        private Boolean isBinaryFile;
        private Integer blockSizeInByte;
        private List<StartEndPair> allWorksForTask;
        private boolean originSkipHeader;
        private MinioClient minioClient;
        private String fileFormat;

        @Override
        public void init() {
            this.readerSliceConfig = this.getPluginJobConf();
            this.fileFormat = this.readerSliceConfig.getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.FILE_FORMAT,
                    com.alibaba.datax.plugin.unstructuredstorage.reader.Constant.DEFAULT_FILE_FORMAT);

            String allWorksForTaskStr = this.readerSliceConfig
                    .getString(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.SPLIT_SLICE_CONFIG);
            if (StringUtils.isBlank(allWorksForTaskStr)) {
                allWorksForTaskStr = "[]";
            }
            this.allWorksForTask = JSON.parseObject(allWorksForTaskStr, new TypeReference<List<StartEndPair>>() {
            });
            this.isBinaryFile = FileFormat.getFileFormatByConfiguration(this.readerSliceConfig).isBinary();
            this.blockSizeInByte = this.readerSliceConfig.getInt(
                    com.alibaba.datax.plugin.unstructuredstorage.reader.Key.BLOCK_SIZE_IN_BYTE,
                    com.alibaba.datax.plugin.unstructuredstorage.reader.Constant.DEFAULT_BLOCK_SIZE_IN_BYTE);
            this.originSkipHeader = this.readerSliceConfig
                    .getBool(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.SKIP_HEADER, false);
        }

        @Override
        public void prepare() {
            LOG.info("task prepare() begin...");
        }


        @Override
        public void startRead(RecordSender recordSender) {
            boolean successOnNoObject = this.readerSliceConfig.getBool(Key.SUCCESS_ON_NO_Object, false);
            if (this.allWorksForTask.isEmpty() && successOnNoObject) {
                recordSender.flush();
                return;
            }
            String bucket = this.readerSliceConfig.getString(Key.BUCKET);
            this.minioClient = initClient(this.readerSliceConfig);
            for (StartEndPair eachSlice : this.allWorksForTask) {
                String object = eachSlice.getFilePath();
                Long start = eachSlice.getStart();
                Long end = eachSlice.getEnd();
                LOG.info(String.format("read bucket=[%s] object=[%s], range: [start=%s, end=%s] start...", bucket,
                        object, start, end));
                InputStream minioInputStream = new MinioInputStream(minioClient, bucket, object, start, end);
                // 检查是否要跳过表头, 防止重复跳过首行
                Boolean skipHeaderValue = this.originSkipHeader && (0L == start);
                this.readerSliceConfig.set(com.alibaba.datax.plugin.unstructuredstorage.reader.Key.SKIP_HEADER,
                        skipHeaderValue);
                try {
                    if (!this.isBinaryFile) {
                        UnstructuredStorageReaderUtil.readFromStream(minioInputStream, object, this.readerSliceConfig,
                                recordSender, this.getTaskPluginCollector());
                    } else {
                        BinaryFileReaderUtil.readFromStream(minioInputStream, object, recordSender, this.blockSizeInByte);
                    }
                } finally {
                    IOUtils.closeQuietly(minioInputStream);
                }
            }
            recordSender.flush();
        }

        @Override
        public void post() {
            LOG.info("task post() begin...");
        }

        @Override
        public void destroy() {
            LOG.info("task destroy() begin...");
        }
    }
}
