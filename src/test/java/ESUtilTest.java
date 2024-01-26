import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.wangrui027.utils.ESUtil;
import lombok.extern.slf4j.Slf4j;
import model.Person;
import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

@Slf4j
public class ESUtilTest {

    private ESUtil util;

    private static final String INDEX_NAME = "es_util_person";

    @BeforeEach
    public void before() {
        String ip = "192.168.101.235";
        int port = 9201;
        String scheme = "http";
        String username = "elastic";
        String password = "123456";
        util = ESUtil.of(e -> e
                .ip(ip)
                .port(port)
                .scheme(scheme)
                .username(username)
                .password(password)
                .indicesName(INDEX_NAME)
        );
    }

    @Test
    public void init() throws IOException {
        boolean exist = util.indicesExist();
        if (exist) {
            indicesDelete();
        }
        indicesCreateWithJson();
        bulkSave();
    }

    @Test
    public void ping() throws IOException {
        boolean ping = util.ping();
        log.info("ping: " + ping);
    }

    @Test
    public void indicesExist() throws IOException {
        boolean exist = util.indicesExist();
        log.info("indicesExist: " + exist);
    }

    @Test
    public void indicesCreate() throws IOException {
        boolean success = util.indicesCreate();
        log.info("indicesCreate: " + success);
    }

    @Test
    public void indicesCreateWithJson() throws IOException {
        String json = IOUtils.toString(Objects.requireNonNull(this.getClass().getResourceAsStream("/person.json")), StandardCharsets.UTF_8);
        boolean success = util.indicesCreateWithJson(json);
        log.info("indicesCreate: " + success);
    }

    @Test
    public void indicesDelete() throws IOException {
        boolean success = util.indicesDelete();
        log.info("indicesDelete: " + success);
    }

    @Test
    public void count() throws IOException {
        long count = util.count();
        log.info("count: " + count);
    }

    @Test
    public void save() throws IOException {
        Person person = new Person()
                .setId(UUID.randomUUID().toString().replace("-", ""))
                .setName("王睿")
                .setAge(35)
                .setCity("武汉")
                .setDescription("王睿是一个java程序员，不会vue")
                .setExtendsInfo(ImmutableMap.of("电话", 18702764000L, "手机型号", "redmi k30s"));
        String _id = util.save(person, "11");
        log.info("doc id: " + _id);
    }

    @Test
    public void bulkSave() throws IOException {
        List<Person> list = ImmutableList.of(
                new Person()
                        .setId(UUID.randomUUID().toString().replace("-", ""))
                        .setName("张三")
                        .setAge(35)
                        .setCity("武汉")
                        .setDescription("张三是一个java程序员，不会vue"),
                new Person()
                        .setId(UUID.randomUUID().toString().replace("-", ""))
                        .setName("李四")
                        .setAge(35)
                        .setCity("北京")
                        .setDescription("李四是一个c++程序员，还会python"),
                new Person()
                        .setId(UUID.randomUUID().toString().replace("-", ""))
                        .setName("王五")
                        .setAge(35)
                        .setCity("北京")
                        .setDescription("王五是一个c++程序员，还会go"),
                new Person()
                        .setId(UUID.randomUUID().toString().replace("-", ""))
                        .setName("赵六")
                        .setAge(35)
                        .setCity("上海")
                        .setDescription("赵六是一个php程序员，不会python")
        );
        BulkResponse response = util.save(list, ImmutableList.of("1", "2", "3", "4"));
        if (!response.errors()) {
            log.info("批量保存准确无误");
        } else {
            log.info("批量保存准确出现错误");
        }
    }

    @Test
    public void getById() throws IOException {
        Person person = util.getById("11", Person.class);
        System.out.println(person);
    }

    @Test
    public void update() throws IOException {
        Person person = new Person()
                .setId(UUID.randomUUID().toString().replace("-", ""))
                .setName("王睿")
                .setAge(35)
                .setCity("武汉")
                .setDescription("王睿是一个java程序员，不会vue")
                .setExtendsInfo(ImmutableMap.of("电话", 18702764000L, "手机型号", "redmi k40"));
        boolean result = util.update("11", person);
        log.info("update: " + result);
    }

    @Test
    public void delete() throws IOException {
        boolean delete = util.delete("11");
        log.info("delete: " + delete);
    }

    @Test
    public void bulkDelete() throws IOException {
        BulkResponse response = util.delete(ImmutableList.of("11", "22"));
        if (!response.errors()) {
            log.info("批量删除准确无误");
        } else {
            log.info("批量删除准确出现错误");
        }
    }

    @Test
    public void search() throws IOException {
        SearchRequest searchRequest = SearchRequest.of(sr -> sr
                .index(INDEX_NAME)
                .q("python")
        );
        List<Person> list = util.search(searchRequest, Person.class);
        System.out.println(list);
    }

}
