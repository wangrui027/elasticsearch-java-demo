package com.wangrui027.utils;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.Result;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.indices.ExistsRequest;
import co.elastic.clients.elasticsearch.indices.*;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import lombok.Builder;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponseInterceptor;
import org.apache.http.entity.ContentType;
import org.apache.http.message.BasicHeader;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * ES 工具封装
 */
@Builder
public class ESUtil {

    /**
     * ES 服务器 IP
     */
    private String ip;

    /**
     * ES 服务器端口号
     */
    @Builder.Default
    private Integer port = 9200;

    /**
     * http 请求协议，不传则为 http
     */
    @Builder.Default
    private String scheme = "http";

    /**
     * ES 服务器用户
     */
    private String username;

    /**
     * ES 服务器密码
     */
    private String password;

    /**
     * 默认索引名，仅操作一个索引时可以对该属性赋值，后续操作即可以不传索引名称
     */
    private String indicesName;

    /**
     * 客户端对象
     */
    private final AtomicReference<ElasticsearchClient> client = new AtomicReference<>(null);

    public static ESUtil of(Function<ESUtilBuilder, ESUtilBuilder> fn) {
        return fn.apply(new ESUtilBuilder()).build();
    }

    /**
     * 获取 ES 客户端
     *
     * @return ES 客户端
     */
    public ElasticsearchClient getClient() {
        if (client.get() == null) {
            RestClientBuilder builder = RestClient
                    .builder(new HttpHost(ip, port, scheme))
                    .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder
                            .setDefaultHeaders(Collections.singletonList(new BasicHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString())))
                            .addInterceptorLast((HttpResponseInterceptor) (request, context) -> request.addHeader("X-Elastic-Product", "Elasticsearch"))
                    );
            if (username != null && password != null) {
                builder.setDefaultHeaders(new Header[]{
                        new BasicHeader("Authorization", "Basic " + Base64.getEncoder().encodeToString((username + ":" + password).getBytes(StandardCharsets.UTF_8))),
                });
            }
            ElasticsearchTransport transport = new RestClientTransport(builder.build(), new JacksonJsonpMapper());
            client.set(new ElasticsearchClient(transport));
        }
        return client.get();
    }

    /**
     * ping ES 服务器
     *
     * @return 是否 ping 通
     * @throws IOException
     */
    public boolean ping() throws IOException {
        BooleanResponse response = getClient().ping();
        return response.value();
    }

    /**
     * 检测索引是否存在
     *
     * @param indicesName 索引名
     * @return 索引是否存在
     * @throws IOException
     */
    public boolean indicesExist(String indicesName) throws IOException {
        ElasticsearchIndicesClient indicesClient = getClient().indices();
        BooleanResponse response = indicesClient.exists(ExistsRequest.of(e -> e.index(indicesName)));
        return response.value();
    }

    /**
     * 检测索引是否存在
     *
     * @return 索引是否存在
     * @throws IOException
     */
    public boolean indicesExist() throws IOException {
        return indicesExist(indicesName);
    }

    /**
     * 创建索引
     *
     * @param indicesName 索引名
     * @return 索引是否创建成功
     * @throws IOException
     */
    public boolean indicesCreate(String indicesName) throws IOException {
        return indicesCreate(indicesName, null);
    }

    /**
     * 创建索引
     *
     * @return 索引是否创建成功
     * @throws IOException
     */
    public boolean indicesCreate() throws IOException {
        return indicesCreate(indicesName);
    }

    /**
     * 创建索引
     *
     * @param indicesName 索引名
     * @param jsonBody    创建索引的请求 body
     * @return 索引是否创建成功
     * @throws IOException
     */
    public boolean indicesCreateWithJson(String indicesName, String jsonBody) throws IOException {
        return indicesCreate(indicesName, jsonBody);
    }

    /**
     * 创建索引
     *
     * @param jsonBody 创建索引的请求 body
     * @return 索引是否创建成功
     * @throws IOException
     */
    public boolean indicesCreateWithJson(String jsonBody) throws IOException {
        return indicesCreate(indicesName, jsonBody);
    }

    /**
     * 创建索引
     *
     * @param indicesName 索引名
     * @param jsonBody    创建索引的请求 body
     * @return 索引是否创建成功
     * @throws IOException
     */
    public boolean indicesCreate(String indicesName, String jsonBody) throws IOException {
        ElasticsearchIndicesClient indicesClient = getClient().indices();
        CreateIndexRequest.Builder builder = new CreateIndexRequest.Builder().index(indicesName);
        if (jsonBody != null) {
            try (InputStream is = new ByteArrayInputStream(jsonBody.getBytes(StandardCharsets.UTF_8))) {
                builder.withJson(is);
            }
        }
        CreateIndexResponse response = indicesClient.create(builder.build());
        return response.acknowledged();
    }

    /**
     * 删除索引
     *
     * @param indicesName 索引名
     * @return 索引是否删除成功
     * @throws IOException
     */
    public boolean indicesDelete(String indicesName) throws IOException {
        ElasticsearchIndicesClient indicesClient = getClient().indices();
        DeleteIndexResponse response = indicesClient.delete(DeleteIndexRequest.of(e -> e.index(indicesName)));
        return response.acknowledged();
    }

    /**
     * 删除索引
     *
     * @return 索引是否删除成功
     * @throws IOException
     */
    public boolean indicesDelete() throws IOException {
        return indicesDelete(indicesName);
    }


    /**
     * 统计索引中的文档总数
     *
     * @param indicesName 索引名
     * @return 文档总数
     * @throws IOException
     */
    public long count(String indicesName) throws IOException {
        if (indicesName == null) {
            return count();
        }
        CountResponse count = getClient().count(e -> e.index(indicesName));
        return count.count();
    }

    /**
     * 统计索引中的文档总数
     *
     * @return 文档总数
     * @throws IOException
     */
    public long count() throws IOException {
        return count(indicesName);
    }

    /**
     * 统计整个 ES 服务器中的文档总数
     *
     * @return 文档总数
     * @throws IOException
     */
    public long countAll() throws IOException {
        CountResponse count = getClient().count();
        return count.count();
    }

    /**
     * 保存文档
     *
     * @param indicesName 索引名
     * @param object      要保存的对象
     * @param id          文档 ID
     * @return 文档 ID
     * @throws IOException
     */
    public <T> String save(String indicesName, T object, String id) throws IOException {
        IndexResponse response = getClient().index(e -> e
                .index(indicesName)
                .id(id)
                .document(object)
        );
        return response.id();
    }

    /**
     * 保存文档
     *
     * @param indicesName 索引名
     * @param object      要保存的对象
     * @return 文档 ID
     * @throws IOException
     */
    public <T> String save(String indicesName, T object) throws IOException {
        return save(indicesName, object, null);
    }

    /**
     * 保存文档
     *
     * @param object 要保存的对象
     * @return 文档 ID
     * @throws IOException
     */
    public <T> String save(T object, String id) throws IOException {
        return save(indicesName, object, id);
    }

    /**
     * 保存文档
     *
     * @param object 要保存的对象
     * @return 文档 ID
     * @throws IOException
     */
    public <T> String save(T object) throws IOException {
        return save(indicesName, object, null);
    }

    /**
     * 更新文档
     *
     * @param indicesName 索引名
     * @param id          文档id
     * @param object      文档对象
     * @return 是否更新成功
     * @throws IOException
     */
    public <T> boolean update(String indicesName, String id, T object) throws IOException {
        UpdateResponse<?> response = getClient().update(UpdateRequest.of(ur -> ur
                .index(indicesName)
                .id(id)
                .doc(object)), object.getClass());
        return Result.Updated.equals(response.result());
    }

    /**
     * 更新文档
     *
     * @param id     文档id
     * @param object 文档对象
     * @return 是否更新成功
     * @throws IOException
     */
    public <T> boolean update(String id, T object) throws IOException {
        return update(indicesName, id, object);
    }

    /**
     * 删除文档
     *
     * @param indicesName 索引名
     * @param id          文档 ID
     * @return 文档是否删除成功
     * @throws IOException
     */
    public boolean delete(String indicesName, String id) throws IOException {
        DeleteResponse response = getClient().delete(e -> e
                .index(indicesName)
                .id(id)
        );
        return Result.Deleted.equals(response.result());
    }

    /**
     * 删除文档
     *
     * @param id 文档 ID
     * @return 文档是否删除成功
     * @throws IOException
     */
    public boolean delete(String id) throws IOException {
        return delete(indicesName, id);
    }

    /**
     * 批量删除文档
     *
     * @param indicesName 索引名
     * @param ids         批量删除的文档 ID 集合
     * @return 批量操作返回对象
     * @throws IOException
     */
    public BulkResponse delete(String indicesName, Collection<String> ids) throws IOException {
        List<BulkOperation> operations = ids.stream().map(id -> BulkOperation.of(bo -> bo
                .delete(e -> e
                        .index(indicesName)
                        .id(id)))
        ).collect(Collectors.toList());
        return bulk(operations);
    }

    /**
     * 批量删除文档
     *
     * @param ids 批量删除的文档 ID 集合
     * @return 批量操作返回对象
     * @throws IOException
     */
    public BulkResponse delete(Collection<String> ids) throws IOException {
        return delete(indicesName, ids);
    }

    /**
     * 批量保存文档
     *
     * @param indicesName 索引名
     * @param list        批量保存的文档集合
     * @return 批量操作返回对象
     * @throws IOException
     */
    public <T> BulkResponse save(String indicesName, List<T> list) throws IOException {
        return save(indicesName, list, Arrays.asList(new String[list.size()]));
    }

    /**
     * 批量保存文档
     *
     * @param indicesName 索引名
     * @param list        批量保存的文档集合
     * @param ids         批量保存的 ID 集合
     * @return 批量操作返回对象
     * @throws IOException
     */
    public <T> BulkResponse save(String indicesName, List<T> list, List<String> ids) throws IOException {
        if (list == null || list.isEmpty()) {
            return null;
        }
        if (ids == null) {
            ids = Arrays.asList(new String[list.size()]);
        } else if (ids.size() != list.size()) {
            throw new RuntimeException("ids 集合大小和 list 集合大小不一致，bulk 中止");
        }
        List<BulkOperation> operations = new ArrayList<>();
        for (int i = 0; i < list.size(); i++) {
            int finalI = i;
            List<String> finalIds = ids;
            BulkOperation operation = BulkOperation.of(bo -> bo.index(e -> e
                    .index(indicesName)
                    .id(finalIds.get(finalI))
                    .document(list.get(finalI))
            ));
            operations.add(operation);
        }
        return bulk(operations);
    }

    /**
     * 批量操作
     *
     * @param operations 批量操作
     * @return 批量操作返回对象
     * @throws IOException
     */
    public BulkResponse bulk(List<BulkOperation> operations) throws IOException {
        return getClient().bulk(BulkRequest.of(br -> br.operations(operations)));
    }

    /**
     * 批量保存文档
     *
     * @param list 批量保存的文档集合
     * @return 批量操作返回对象
     * @throws IOException
     */
    public <T> BulkResponse save(List<T> list) throws IOException {
        return save(indicesName, list, Arrays.asList(new String[list.size()]));
    }

    /**
     * 批量保存文档
     *
     * @param list 批量保存的文档集合
     * @param ids  批量保存的 ID 集合
     * @return 批量操作返回对象
     * @throws IOException
     */
    public <T> BulkResponse save(List<T> list, List<String> ids) throws IOException {
        return save(indicesName, list, ids);
    }

    /**
     * 通过 ID 检索文档
     *
     * @param indicesName 索引名
     * @param id          文档 ID
     * @param tClass      返回的对象类型
     * @return 文档对象
     * @throws IOException
     */
    public <T> T getById(String indicesName, String id, Class<T> tClass) throws IOException {
        GetResponse<T> response = getClient().get((gr -> gr.index(indicesName).id(id)), tClass);
        return response.source();
    }

    /**
     * 通过 ID 检索文档
     *
     * @param id     文档 ID
     * @param tClass 返回的对象类型
     * @return 文档对象
     * @throws IOException
     */
    public <T> T getById(String id, Class<T> tClass) throws IOException {
        return getById(indicesName, id, tClass);
    }

    /**
     * 通条件检索文档
     *
     * @param searchRequest 检索条件
     * @param tClass        返回的对象类型
     * @return 文档对象集合
     * @throws IOException
     */
    public <T> List<T> search(SearchRequest searchRequest, Class<T> tClass) throws IOException {
        SearchResponse<T> response = getClient().search(searchRequest, tClass);
        return response.hits().hits().stream().map(Hit::source).collect(Collectors.toList());
    }

}
