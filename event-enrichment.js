// Learn more about destination functions API at
// https://segment.com/docs/connections/destinations/destination-functions

/**
 * Handle track event
 * @param  {SegmentTrackEvent} event
 * @param  {FunctionSettings} settings
 */

//Function for generating JWT token with access to Google Firestore
async function generateJwtToken(settings) {
	// Permissions to request for Access Token
	let scopes = 'https://www.googleapis.com/auth/datastore';

	// Set how long this token will be valid in seconds
	let expiresIn = 3600; // Expires in 60 minutes

	// Google Endpoint for creating OAuth 2.0 Access Tokens from Signed-JWT
	authUrl = 'https://www.googleapis.com/oauth2/v4/token';
	let issued = Math.floor(Date.now() / 1000); // Date.now returns time in ms but we need it to be in seconds.
	let expires = issued + expiresIn;
	// JWT Headers
	const additionalHeaders = {
		kid: settings.privateKeyId,
		alg: 'RS256',
		typ: 'JWT' // Google uses SHA256withRSA
	};

	var buff = new Buffer.from(JSON.stringify(additionalHeaders));
	let base64header = buff.toString('base64');

	// JWT Payload
	const payload = {
		iss: settings.clientEmail, // Issuer claim
		sub: settings.clientEmail, // Issuer claim
		aud: authUrl, // Audience claim
		iat: issued, // Issued At claim
		exp: expires, // Expire time
		scope: scopes // Permissions
	};

	var buff = new Buffer.from(JSON.stringify(payload));
	let base64payload = buff.toString('base64');

	// Defining the algorithm to be used
	const algo = 'RSA-SHA256';
	// Update private key line breaks
	const updatedPrivateKey = settings.privateKey.replace(/\\n/gm, '\n');
	// Creating Sign object
	var sign = crypto.createSign(algo);
	sign.update(base64header + '.' + base64payload);
	let signature = sign.sign(updatedPrivateKey, 'base64');

	let signedJwt = base64header + '.' + base64payload + '.' + signature;
	if (!signedJwt) {
		throw Error(`Empty JWT token`);
	}
	return signedJwt;
}

// Function for getting an access token for Google Cloud
async function exchangeJwtForAccessToken(signedJwt) {
	var urlencoded = new URLSearchParams();
	urlencoded.append(
		'grant_type',
		'urn:ietf:params:oauth:grant-type:jwt-bearer'
	);
	urlencoded.append('assertion', signedJwt);

	var requestOptions = {
		method: 'POST',
		headers: {
			'Content-Type': 'application/x-www-form-urlencoded'
		},
		body: urlencoded,
		redirect: 'follow'
	};
	let response = await fetch(
		'https://oauth2.googleapis.com/token',
		requestOptions
	);
	if (response.status !== 200) {
		throw Error(`accessToken status ${response.status}`);
	}
	let responseJson = await response.json();
	var acess_token = await responseJson.access_token;
	expires_at = Math.floor(Date.now() / 1000) + responseJson.expires_in;
	if (!acess_token) {
		throw Error(`Empty access_token ${JSON.stringify(responseJson)}`);
	}
	return [acess_token, expires_at];
}

// Function for getting email from Personas
async function getEmailFromPersonas(
	segmentPersonasSpaceId,
	segmentPersonasAccessToken,
	userId
) {
	var requestUrl =
		'https://profiles.segment.com/v1/spaces/' +
		segmentPersonasSpaceId +
		'/collections/users/profiles/user_id:' +
		userId +
		'/traits?include=email,phone';
	var base64SegmentPersonasAccessToken = btoa(
		segmentPersonasAccessToken + ': '
	);

	var requestOptions = {
		method: 'GET',
		headers: {
			Authorization: 'Basic ' + base64SegmentPersonasAccessToken
		},
		redirect: 'follow'
	};
	let response = await fetch(requestUrl, requestOptions);
	// If we get a status 200 the user excist in Personas.
	if (response.status == 200) {
		let responseJson = await response.json();
		var responseTraits = responseJson.traits;
	} else if (response.status !== 200) {
		// If it is a new lead they will not be found in personas. We want to handle them as is they did not have any values.
		if (response.status == 404 && response.statusText == 'Not Found') {
			var responseTraits = {};
		} else {
			// if we get a non status 200 for another reason than user not found, we will throw an error
			throw Error(
				`getSfmcIdAndPermissionDataFromPersonas function failed with status: ${response.status}`
			);
		}
	}
	return responseTraits;
}

// Function for getting exchange rates
async function getLocalToDKKCurrencyMultiplier(localCurrency) {
	var requestOptions = {
		method: 'GET',
		redirect: 'follow'
	};
	let response = await fetch(
		`https://api.exchangeratesapi.io/latest?base=${localCurrency}&symbols=DKK`,
		requestOptions
	);
	if (response.status !== 200) {
		throw Error(`exchangerate status ${response.status}`);
	}
	let responseJson = await response.json();
	var LocalToDKKCurrencyMultiplier =
		responseJson.rates[Object.keys(responseJson.rates)[0]];
	if (!LocalToDKKCurrencyMultiplier) {
		LocalToDKKCurrencyMultiplier = 1;
	}
	return LocalToDKKCurrencyMultiplier;
}

// This function replaces local revenue, price and currency with values in DKK
async function replaceLocalCurrencywithDKK(body) {
	if (
		Object.keys(body.properties).includes('currency') &&
		(Object.keys(body.properties).includes('revenue') ||
			Object.keys(body.properties).includes('price'))
	) {
		if (body.properties.currency != 'DKK') {
			var localCurrency = body.properties.currency;
			LocalToDKKCurrencyMultiplier = await getLocalToDKKCurrencyMultiplier(
				localCurrency
			);
		}
	} else if (Object.keys(body.properties).includes('products')) {
		if (body.properties.products[0].currency != 'DKK') {
			var localCurrency = body.properties.products[0].currency;
			LocalToDKKCurrencyMultiplier = await getLocalToDKKCurrencyMultiplier(
				localCurrency
			);
		}
	} else {
		LocalToDKKCurrencyMultiplier = 1;
	}
	// Replace local value with DKK value for the different ways the currency can be sent.
	if (Object.keys(body.properties).includes('revenue')) {
		body.properties['revenue'] = parseInt(
			body.properties['revenue'] * LocalToDKKCurrencyMultiplier,
			10
		);
	}
	if (Object.keys(body.properties).includes('price')) {
		body.properties['price'] = parseInt(
			body.properties['price'] * LocalToDKKCurrencyMultiplier,
			10
		);
	}

	if (Object.keys(body.properties).includes('products')) {
		for (product in body.properties.products) {
			if (Object.keys(body.properties.products[product]).includes('revenue')) {
				body.properties.products[product]['revenue'] = parseInt(
					body.properties.products[product]['revenue'] *
						LocalToDKKCurrencyMultiplier,
					10
				);
			}
			if (Object.keys(body.properties.products[product]).includes('price')) {
				body.properties.products[product]['price'] = parseInt(
					body.properties.products[product]['price'] *
						LocalToDKKCurrencyMultiplier,
					10
				);
			}
			if (Object.keys(body.properties.products[product]).includes('currency')) {
				body.properties.products[product]['currency'] = 'DKK';
			}
		}
	}
	if (Object.keys(body.properties).includes('currency')) {
		body.properties['currency'] = 'DKK';
	}
	return body;
}

//Function for getting product data from Firestore
async function get_product_data(productDataOriginal, acessToken) {
	const response = await fetch(
		'https://firestore.googleapis.com/v1beta1/projects/your-gcp-project/databases/(default)/documents/Products/' +
			productDataOriginal.product_id,
		{
			headers: {
				Authorization: 'Bearer ' + acessToken
			}
		}
	);

	if (response.status !== 200) {
		// If the product is not found we still want to send the event.
		if (response.status === 404 && response.statusText === 'Not Found') {
			product_data = {};
		} else {
			throw Error(
				`Firestre API status ${response.status} (Reason: ${response.statusText}) for the following product_id: ${productDataOriginal.product_id}`
			);
		}
	} else {
		response_json = await response.json();
		var fields = await response_json.fields;
		var product_data = {};
		// Firestore sends the field type and the value in a nested object. The for loop extracts the value and add it to product_data with the field name.
		for (var prop in fields) {
			product_data[prop] = Object.values(fields[prop])[0];
		}
		product_data['cost'] = parseInt(product_data['cost'], 10);
	}

	return product_data;
}

// Function that adds new product information to original product information
async function addProductDataToEvent(originalProductData, productData) {
	if (
		Object.keys(productData).length !== 0 &&
		productData.constructor === Object
	) {
		// Remove cost if we don't have revenue or price in the event
		if (
			Object.keys(originalProductData).includes('revenue') == false &&
			Object.keys(originalProductData).includes('price') == false
		) {
			if (Object.keys(productData).includes('cost')) {
				delete productData.cost;
			}
		}
		for (var prop in productData) {
			originalProductData[prop] = productData[prop];
		}
		var updatedProductData = originalProductData;
	} else {
		// In case the product data was not found in Firestore
		var updatedProductData = originalProductData;
	}
	return updatedProductData;
}

// Function that fetches and merges new product data to event with a products array with multiple products
async function multipleProductsFunction(
	productDataOriginal,
	acessToken = acessToken
) {
	var productData = await get_product_data(productDataOriginal, acessToken);
	var productDataUpdated = await addProductDataToEvent(
		productDataOriginal,
		productData
	);
	// If the event contains a quantity we want to multiply it to the margin
	if (Object.keys(productDataUpdated).includes('quantity')) {
		//Check if product contains revenue or price and use it for margin calculation
		if (Object.keys(productDataUpdated).includes('revenue')) {
			productDataUpdated.margin = parseFloat(
				(
					(productDataUpdated.revenue - productDataUpdated.cost) *
					productDataUpdated.quantity
				).toFixed(2)
			);
		} else if (Object.keys(productDataUpdated).includes('price')) {
			productDataUpdated.margin = parseFloat(
				(
					(productDataUpdated.price - productDataUpdated.cost) *
					productDataUpdated.quantity
				).toFixed(2)
			);
		}
	} else {
		//Check if product contains revenue or price and use it for margin calculation
		if (Object.keys(productDataUpdated).includes('revenue')) {
			productDataUpdated.margin = parseFloat(
				(productDataUpdated.revenue - productDataUpdated.cost).toFixed(2)
			);
		} else if (Object.keys(productDataUpdated).includes('price')) {
			productDataUpdated.margin = parseFloat(
				(productDataUpdated.price - productDataUpdated.cost).toFixed(2)
			);
		}
	}
	if (isNaN(productDataUpdated.margin)) {
		delete productDataUpdated.margin;
	}
	return productDataUpdated;
}

// Function that sends the updated event to Segment's HTTP API
async function sendDataToSegmentHttpApi(body, httpApiKey) {
	var base64Authentication = btoa(httpApiKey + ': ');
	var rawResponse = await fetch('https://api.segment.io/v1/track', {
		method: 'POST',
		headers: {
			Authorization: 'Basic ' + base64Authentication,
			'Content-Type': 'application/json'
		},
		body: JSON.stringify(body)
	});
	if (rawResponse.status !== 200) {
		throw Error(
			`Segment http API status ${rawResponse.status}. Reason: ${rawResponse.statusText}`
		);
	}
}

//Main function that is excecuted
async function onTrack(event, settings) {
	var body = event;
	delete body.messageId;

	// If access token is not defined - Generate new
	if (typeof acessToken === 'undefined') {
		var signedJwt = await generateJwtToken(settings);
		var result = await exchangeJwtForAccessToken(signedJwt);
		var acessToken = result[0];
		var expires_at = result[1];
		// If expires_at is not defined - Get new access token
	} else if (typeof expires_at === 'undefined') {
		var signedJwt = await generateJwtToken(settings);
		var result = await exchangeJwtForAccessToken(signedJwt);
		var acessToken = result[0];
		var expires_at = result[1];
		// If the current access token expires in less than 10 seconds - Generate new access token
	} else if (Math.floor(Date.now() / 1000) - expires_at < 10) {
		var signedJwt = await generateJwtToken(settings);
		var result = await exchangeJwtForAccessToken(signedJwt);
		var acessToken = result[0];
		var expires_at = result[1];
	}

	// Some of our Server side pixel destinations require that events sent to it contains context.traits.email
	// If the event body contains a userId - Look up via the Personas Profile API if we now the users email
	if (Object.keys(body).includes('userId')) {
		if (body.userId !== null) {
			var context_traits = await getEmailFromPersonas(
				settings.segmentPersonasSpaceId,
				settings.segmentPersonasAccessToken,
				body.userId
			);
			// If the API call to Personas returned an email - Add it to context.traits
			if (Object.keys(context_traits).length !== 0) {
				body.context['traits'] = context_traits;
			}
		}
	}

	// Check if the currency is different from DKK and we have price or revenue and convert to DKK
	// If we need to convert revenue and/or price to DKK we run the Google Cloud access token request in parrallel
	if (
		Object.keys(body.properties).includes('currency') &&
		(Object.keys(body.properties).includes('revenue') ||
			Object.keys(body.properties).includes('price'))
	) {
		if (body.properties.currency != 'DKK') {
			body = await replaceLocalCurrencywithDKK(body);
		}
	} else if (Object.keys(body.properties).includes('products')) {
		if (body.properties['products'].length !== 0) {
			if (Object.keys(body.properties.products[0]).includes('currency')) {
				if (body.properties.products[0].currency != 'DKK') {
					body = await replaceLocalCurrencywithDKK(body);
				}
			}
		}
	}

	// Add Product data for events with a product array with multiple products
	if (Object.keys(body.properties).includes('products')) {
		if (body.properties['products'].length !== 0) {
			var multipleProductsDataOriginal = body.properties.products;
			productRequests = [];
			for (product in multipleProductsDataOriginal) {
				productRequests.push(
					multipleProductsFunction(
						multipleProductsDataOriginal[product],
						acessToken
					)
				);
			}
			updatedProductsArray = await Promise.all(productRequests);
			body.properties.products = updatedProductsArray;
			if (
				Object.keys(body.properties).includes('revenue') &&
				Object.keys(body.properties.products[0]).includes('margin')
			) {
				var marginTotal = body.properties.products.reduce(function(prev, cur) {
					return prev + cur.margin;
				}, 0);
				body.properties['margin'] = marginTotal;
			}
		}

		// Add product data for events with a single event
	} else if (Object.keys(body.properties).includes('product_id')) {
		var productDataOriginal = body.properties;
		var productData = await get_product_data(productDataOriginal, acessToken);
		body.properties = await addProductDataToEvent(
			productDataOriginal,
			productData
		);
		body.properties.margin = (
			(body.properties.price - body.properties.cost) *
			body.properties.quantity
		).toFixed(2);
		if (isNaN(body.properties.margin)) {
			delete body.properties.margin;
		}
	}
	console.log(body);
	await sendDataToSegmentHttpApi(body, settings.httpApiKey);
}
