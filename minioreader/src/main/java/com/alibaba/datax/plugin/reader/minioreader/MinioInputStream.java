package com.alibaba.datax.plugin.reader.minioreader;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.RetryUtil;
import io.minio.GetObjectArgs;
import io.minio.GetObjectResponse;
import io.minio.MinioClient;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Callable;

/**
 * @Author: guxuan
 * @Date 2022-05-17 15:52
 */
public class MinioInputStream extends InputStream {

    private final MinioClient minioClient;
    private GetObjectArgs getObjectRequest;

    private long startIndex = 0;
    private long endIndex = -1;

    private InputStream inputStream;

    /**
     * retryTimes : 重试次数, 默认值是60次;
     * description: 能够cover住的网络断连时间= retryTimes*(socket_timeout+sleepTime);
     *              默认cover住的网络断连时间= 60*(5+5) = 600秒.
     */
    private int retryTimes = 60;

    private static final Logger LOG = LoggerFactory.getLogger(MinioInputStream.class);

    /**
     * 如果start为0, end为1000, inputstream范围是[0,1000],共1001个字节
     *
     * @param minioClient
     * @param bucket
     * @param object
     * @param start inputstream start index
     * @param end inputstream end index
     */
    public MinioInputStream(final MinioClient minioClient, final String bucket, final String object, long start, long end) {
        this.minioClient = minioClient;

        this.startIndex = start;
        this.endIndex = end;
        Long len = this.endIndex == -1 ? null : this.endIndex - this.startIndex + 1;
        this.getObjectRequest = GetObjectArgs.builder()
                .bucket(bucket)
                .object(object)
                .offset(this.startIndex)
                .length(len)
                .build();
        try {
            RetryUtil.executeWithRetry(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    // 读取InputStream
                    inputStream = minioClient.getObject(getObjectRequest);
                    return true;
                }
            }, this.retryTimes, 5000, false);
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    MinioReaderErrorCode.RUNTIME_EXCEPTION,e.getMessage(), e);
        }
    }

    public MinioInputStream(final MinioClient minioClient, final String bucket, final String object) {
        this.minioClient = minioClient;
        this.getObjectRequest = GetObjectArgs.builder()
                .bucket(bucket)
                .object(object)
                .build();
        try {
            RetryUtil.executeWithRetry(new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    // 读取InputStream
                    inputStream = minioClient.getObject(getObjectRequest);
                    return true;
                }
            }, this.retryTimes, 5000, false);
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    MinioReaderErrorCode.RUNTIME_EXCEPTION, e.getMessage(), e);
        }
    }

    @Override
    public int read() throws IOException {
        int cbyte;
        try {
            cbyte = RetryUtil.executeWithRetry(new Callable<Integer>() {
                @Override
                public Integer call() throws Exception {
                    try {
                        int c = inputStream.read();
                        startIndex++;
                        return c;
                    } catch (Exception e) {
                        LOG.warn(e.getMessage(),e);
                        /**
                         * 必须将inputStream先关闭, 否则会造成连接泄漏
                         */
                        IOUtils.closeQuietly(inputStream);
                        // getOssRangeInuptStream时,如果网络不连通,则会抛出异常,RetryUtil捕获异常进行重试
                        inputStream = getOssRangeInuptStream(startIndex);
                        int c = inputStream.read();
                        startIndex++;
                        return c;
                    }
                }
            }, this.retryTimes,5000, false);
            return cbyte;
        } catch (Exception e) {
            throw DataXException.asDataXException(
                    MinioReaderErrorCode.RUNTIME_EXCEPTION, e.getMessage(), e);
        }
    }

    private InputStream getOssRangeInuptStream(final long startIndex) throws Exception{
        LOG.info("Start to retry reading [inputStream] from Byte {}", startIndex);
        // 第二个参数值设为-1，表示不设置结束的字节位置,读取startIndex及其以后的所有数据
        getObjectRequest = GetObjectArgs.builder()
                .bucket(getObjectRequest.bucket())
                .object(getObjectRequest.object())
                .offset(startIndex)
                .length(this.endIndex - startIndex + 1)
                .build();
        // 范围下载
        GetObjectResponse objectResponse = minioClient.getObject(getObjectRequest);
        // 读取InputStream
        LOG.info("Start to retry reading [inputStream] from Byte {}", startIndex);
        return objectResponse;
    }
}
