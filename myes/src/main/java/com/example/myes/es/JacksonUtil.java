package com.example.myes.es;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.json.JsonReadFeature;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.deser.std.StringDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.vavr.control.Try;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.text.DateFormat;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * jackson 工具类
 *
 * @author shizeying
 * @date 2021/06/12
 */
@Slf4j
public class JacksonUtil {
	
	private static ObjectMapper mapper = new ObjectMapper();
	
	public static String bean2Json(Object obj) {
		DateFormat dateFormat = mapper.getDateFormat();
		mapper.disable(SerializationFeature.WRITE_DATE_KEYS_AS_TIMESTAMPS).setSerializationInclusion(JsonInclude.Include.NON_EMPTY)
		
		;
		return Try.of(() -> mapper.writerWithDefaultPrettyPrinter().writeValueAsString(obj))
		          .onFailure(error -> log.error("JacksonUtil:[{}]", error.getMessage())).get();
	}
	
	

	
	public static <T> T json2BeanByType(byte[] data, Class<T> clazz) {
		
		return Try.of(() -> mapper.readValue(data, clazz)).onFailure(error -> log.error("JacksonUtil:[{}]", error.getMessage())).get();
	}
	
	public static <T> T json2BeanByTypeReference(String jsonStr, TypeReference<T> toValueTypeRef) {
		mapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
		return Try.of(() -> mapper.readValue(jsonStr, toValueTypeRef)).onFailure(error -> log.error("JacksonUtil:[{}]", error.getMessage())).get();
	}
	
	public static <T> T json2LongByTypeReference(String jsonStr, TypeReference<T> toValueTypeRef) {
		return Try.of(() -> mapper.readValue(jsonStr, toValueTypeRef)).onFailure(error -> log.error("JacksonUtil:[{}]", error.getMessage())).get();
	}
	
	public static <T> Map convertValue(T entity) {
		return mapper.convertValue(entity, Map.class);
	}
	
	public static JsonNode readJson(String content) {
		mapper.disable(SerializationFeature.WRITE_DATE_KEYS_AS_TIMESTAMPS);
		return Try.of(() -> mapper.configure(JsonReadFeature.ALLOW_UNESCAPED_CONTROL_CHARS.mappedFeature(), true).readTree(content))
		          .getOrElseThrow(JsonNoSuchElementException::new);
		
	}
	
	
	public static Boolean readJsonError(String content) {
		if (StringUtils.isAllBlank(content)){
			return false;
		}
		
		try {
			SimpleModule module = new SimpleModule();
			module.addDeserializer(String.class, new StdDeserializer<String>(String.class) {
				@Override
				public String deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
					String result = StringDeserializer.instance.deserialize(p, ctxt);
					if (StringUtils.isEmpty(result)) {
						return null;
					}
					return result;
				}
			});
			mapper.registerModule(module);
			mapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true).readTree(content);
			return true;
		} catch (Throwable e) {
			return false;
		}
	}
	
	public static Supplier<Stream<JsonNode>> readJsonStream(String content) {
		return () -> StreamSupport.stream(Try.of(() -> mapper.readTree(content)).getOrElseThrow(JsonNoSuchElementException::new).spliterator(),
				false);
	}
	
	
	public static ObjectNode creatObjectNode() {
		return mapper.createObjectNode();
	}
	
	public static ArrayNode createArray() {
		return mapper.createArrayNode();
	}
	
	
}