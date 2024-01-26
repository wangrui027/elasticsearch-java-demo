package model;

import lombok.Data;
import lombok.experimental.Accessors;

import java.util.Map;

@Data
@Accessors(chain = true)
public class Person {

    private String id;
    private String name;
    private Integer age;
    private String city;
    private String description;
    private Map<String, Object> extendsInfo;

}
