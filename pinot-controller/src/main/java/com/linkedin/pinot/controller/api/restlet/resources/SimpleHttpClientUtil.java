package com.linkedin.pinot.controller.api.restlet.resources;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpParams;
import org.apache.http.protocol.HTTP;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;

public class SimpleHttpClientUtil {
	public static String getStringFromStream(InputStream input) {
		String body = null;
		try {
			ByteArrayOutputStream bao = new ByteArrayOutputStream(512);
			byte[] bb = new byte[512];
			int len = 0;
			while ((len = input.read(bb)) > 0) {
				bao.write(bb, 0, len);
			}
			body = bao.toString();
		} catch (Exception e) {
			body = "";
			e.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}

		return body;
	}

	public static String requestGet(String url) throws Exception {
		HttpClient httpclient = new DefaultHttpClient();
		HttpGet httpgets = new HttpGet(url);
		HttpResponse response = httpclient.execute(httpgets);
		HttpEntity entity = response.getEntity();
		if (entity != null) {
			InputStream instreams = entity.getContent();
			String str = getStringFromStream(instreams);
			// Do not need the rest
			httpgets.abort();
			return str;
		}
		return "";
	}

	public static String requestPost(String url, String content) throws Exception {
		HttpClient httpclient = new DefaultHttpClient();
		HttpPost httppost = new HttpPost(url);
		HttpEntity requestEntity = new StringEntity(content, HTTP.UTF_8);
		httppost.setEntity(requestEntity);
		HttpResponse response = httpclient.execute(httppost);
		HttpEntity responseEntity = response.getEntity();
		if (responseEntity != null) {
			InputStream instreams = responseEntity.getContent();
			String str = getStringFromStream(instreams);
			// Do not need the rest
			httppost.abort();
			return str;
		}
		return "";
	}


  public static String requestPostQuery(String url, String content) throws Exception {
    HttpClient httpclient = new DefaultHttpClient();
    HttpPost httppost = new HttpPost(url);
    final byte[] requestBytes = content.getBytes("UTF-8");
    httppost.setEntity(new ByteArrayEntity(requestBytes));
    HttpParams params = new BasicHttpParams();
//    params.setParameter("Accept-Encoding", "gzip");
//    params.setParameter("Content-Length", String.valueOf(requestBytes.length));
//    params.setParameter("http.keepAlive", String.valueOf(true));
//    params.setParameter("default", String.valueOf(true));
    HttpResponse response = httpclient.execute(httppost);
    HttpEntity responseEntity = response.getEntity();
    if (responseEntity != null) {
      InputStream instreams = responseEntity.getContent();
      String str = getStringFromStream(instreams);
      // Do not need the rest
      httppost.abort();
      return str;
    }
    return "";
  }

  public static void main(String[] args) throws Exception {
    System.out.println(SimpleHttpClientUtil.requestPost("http://192.168.11.148:8099/query", "{}"));
  }
}