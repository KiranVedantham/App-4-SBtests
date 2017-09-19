package com.swisscom.api.controllers;

import java.util.Calendar;
import java.util.Date;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.swisscom.api.AllServiceConfiguration;

@RestController
@RequestMapping("/analytics")
public class AllConnectionsController {

	@Autowired
	private AllServiceConfiguration sc;

	@SuppressWarnings("unused")
	@RequestMapping(value = "/cons/{numConns}", method = RequestMethod.GET)
	public ResponseEntity<String> createConnections(@PathVariable String numConns) throws Exception {
		int numberOfconns = Integer.parseInt(numConns);
		Date t = Calendar.getInstance().getTime();
		System.out.println("Started Creating cons : "+Calendar.getInstance().getTime());
		for (int i = 1; i <= numberOfconns; i++) {
			Object fisrtc = null;
			try {
				fisrtc = sc.getServiceInstance();
//				System.out.println("Connection Number : " + i);
			} catch (Exception e) {
				System.out.println(" Failed at Connection Number : " + i);
				throw e;
			}
		}
		System.out.println("Started Creating cons : "+t);

		System.out.println("completed Creating cons : "+Calendar.getInstance().getTime());
		
		return new ResponseEntity<String>(numConns,HttpStatus.OK);
	}
}
