package com.example.myes.es;

import java.util.NoSuchElementException;

public class JsonNoSuchElementException extends NoSuchElementException {


	public JsonNoSuchElementException() {
		super("json解析错误");
	}
}
