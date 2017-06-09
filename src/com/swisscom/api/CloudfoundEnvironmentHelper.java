package com.swisscom.api;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import com.amazonaws.util.json.JSONException;
import com.jayway.jsonpath.JsonPath;

import net.minidev.json.JSONArray;

public class CloudfoundEnvironmentHelper {
	@SuppressWarnings("unchecked")
	public static Optional<Map<String, Object>> parseCredentials(String vcapServices,
			final Optional<String> optionalTargetTag) {
		if (vcapServices != null) {
			List<Map<String, Object>> services = JsonPath.parse(vcapServices).read("$.*.[?(@.credentials)]",
					List.class);
			return services.stream().filter(o -> {
				Collection<String> tags = (Collection<String>) o.get("tags");
				if (optionalTargetTag.isPresent()) {
					return tags != null && tags.contains(optionalTargetTag.get());
				} else {
					return true;
				}
			}).findFirst().map(t -> (Map<String, Object>) t.get("credentials"));

		}
		return Optional.empty();
	}

	@SuppressWarnings("unchecked")
	public static String parseLabels(String vcapServices) throws JSONException {
		if (vcapServices != null) {
			JSONArray services = JsonPath.parse(vcapServices).read("$.*.[?(@.label)]");
			Map<String, Object> dbLabel = (Map<String, Object>) services.get(0);
			return (String) dbLabel.get("label");
		}
		return null;
	}

}
