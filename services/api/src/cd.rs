//! Internal client for the ČD Ticket API 1.0.0.
//!
//! Upstream identifiers and credentials in this module are never serialized by
//! the app-facing API. Responses are retained as JSON in the ticketing store so
//! newly added upstream fields are not silently discarded.

use std::{collections::BTreeMap, fmt, sync::Arc, time::Duration};

use async_trait::async_trait;
use base64::{Engine, engine::general_purpose::STANDARD};
use percent_encoding::{NON_ALPHANUMERIC, utf8_percent_encode};
use reqwest::{
    Method, StatusCode,
    header::{HeaderMap, HeaderName, HeaderValue},
};
use rsa::{
    RsaPrivateKey,
    pkcs1::DecodeRsaPrivateKey,
    pkcs1v15::SigningKey,
    pkcs8::DecodePrivateKey,
    signature::{SignatureEncoding, Signer},
};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use sha1::Sha1;
use thiserror::Error;
use zeroize::{Zeroize, ZeroizeOnDrop};

const X_USER: HeaderName = HeaderName::from_static("x-user");
const X_DESC: HeaderName = HeaderName::from_static("x-desc");
const X_LANG: HeaderName = HeaderName::from_static("x-lang");
const X_HASH: HeaderName = HeaderName::from_static("x-hash");
const MAX_DOCUMENT_BYTES: u64 = 25 * 1024 * 1024;

#[derive(Clone, Zeroize, ZeroizeOnDrop)]
pub struct Secret(String);

impl Secret {
    pub fn new(value: String) -> Self {
        Self(value)
    }
    pub(crate) fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for Secret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("[REDACTED]")
    }
}

#[derive(Clone)]
pub struct CdConfig {
    pub base_url: String,
    pub partner_user: Secret,
    pub private_key_pem: Secret,
    pub description: String,
    pub language: Language,
    pub timeout: Duration,
}

impl fmt::Debug for CdConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CdConfig")
            .field("base_url", &self.base_url)
            .field("partner_user", &self.partner_user)
            .field("private_key_pem", &self.private_key_pem)
            .field("description", &self.description)
            .field("language", &self.language)
            .field("timeout", &self.timeout)
            .finish()
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Language {
    Cs,
    En,
    De,
}

impl Language {
    fn as_str(self) -> &'static str {
        match self {
            Self::Cs => "cs",
            Self::En => "en",
            Self::De => "de",
        }
    }
}

#[derive(Debug, Error)]
pub enum CdError {
    #[error("ČD integration is not configured")]
    NotConfigured,
    #[error("invalid ČD signing key")]
    InvalidSigningKey,
    #[error("ČD request timed out")]
    Timeout,
    #[error("ČD service is unavailable")]
    Unavailable,
    #[error("ČD rejected the request ({status}, code {upstream_code:?})")]
    Rejected {
        status: u16,
        upstream_code: Option<i32>,
        diagnostic: Option<String>,
    },
    #[error("ČD returned a malformed response")]
    MalformedResponse,
}

impl CdError {
    pub fn stable_code(&self) -> &'static str {
        match self {
            Self::NotConfigured => "ticketing_unavailable",
            Self::InvalidSigningKey => "ticketing_configuration_error",
            Self::Timeout => "upstream_timeout",
            Self::Unavailable => "upstream_unavailable",
            Self::Rejected {
                status: 401 | 403, ..
            } => "upstream_authentication_error",
            Self::Rejected { status: 404, .. } => "upstream_resource_expired",
            Self::Rejected { .. } => "upstream_rejected",
            Self::MalformedResponse => "upstream_malformed_response",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Location {
    pub name: Option<String>,
    pub r#type: Option<i32>,
    pub type_name: Option<String>,
    pub state: Option<String>,
    pub region: Option<String>,
    pub key: Option<i32>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct Locations {
    #[serde(default)]
    pub data: Vec<Location>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SearchConnectionInfo {
    pub result: Option<i32>,
    pub handle: Option<i32>,
    pub conn_info: Option<ConnectionListInfo>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionListInfo {
    pub allow_prev: Option<bool>,
    pub allow_next: Option<bool>,
    #[serde(default)]
    pub connections: Vec<ConnectionInfo>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionInfo {
    pub id: Option<i32>,
    #[serde(default)]
    pub trains: Vec<Value>,
    pub remarks: Option<Value>,
    pub price_offers: Option<Value>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

macro_rules! preserved_type {
    ($name:ident) => {
        #[derive(Debug, Clone, Serialize, Deserialize, Default)]
        pub struct $name {
            #[serde(flatten)]
            pub fields: BTreeMap<String, Value>,
        }
    };
}

preserved_type!(PriceOffersInfo);
preserved_type!(FixedPriceOfferInfo);
preserved_type!(ReservationsInfo);
preserved_type!(BikeRoutePrice);
preserved_type!(LuggageDogPrice);
preserved_type!(AvailableAdditionalServices);
preserved_type!(CoachesSchema);
preserved_type!(SellInfo);
preserved_type!(RefundResult);
preserved_type!(RefundsInfos);
preserved_type!(PassengerMapData);

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct BookingRequest {
    pub flags: Option<i32>,
    pub class: Option<i32>,
    #[serde(default)]
    pub passengers: Vec<Passenger>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Passenger {
    pub count: Option<i32>,
    pub id: Option<i32>,
    pub age: Option<i32>,
    pub card: Option<i32>,
    pub card_number: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct ConnectionParameters {
    pub max_change: Option<i32>,
    pub max_time: Option<i32>,
    pub min_time: Option<i32>,
    pub use_beds: Option<bool>,
    pub delta_max: Option<i32>,
    #[serde(default)]
    pub fc_search_ids: Vec<i32>,
    #[serde(default)]
    pub tr_type_ids: Vec<i32>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Debug, Clone)]
pub struct SearchConnectionsRequest {
    pub from_type: i32,
    pub from: i32,
    pub to_type: i32,
    pub to: i32,
    pub via_type: Option<i32>,
    pub via: Option<i32>,
    pub change_type: Option<i32>,
    pub change: Option<i32>,
    pub date_time: Option<String>,
    pub previous: bool,
    pub max_count: i32,
    pub parameters: ConnectionParameters,
    pub booking: BookingRequest,
}

#[derive(Debug, Clone)]
pub struct PageRequest {
    pub conn_id: Option<i32>,
    pub previous: bool,
    pub listed_count: i32,
    pub max_count: i32,
    pub booking: BookingRequest,
}

#[derive(Debug, Clone)]
pub struct CustomerInfo {
    pub email: String,
    pub name: Option<String>,
    pub in_card_number: Option<i64>,
    pub birth_date: Option<String>,
    pub company_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SchemaRequest {
    pub flags: i32,
    pub schema_info: Value,
    pub coach_number: Option<String>,
    #[serde(default)]
    pub self_reserved_seats: Vec<i32>,
    pub schema_width: i32,
    pub schema_max_height: Option<i32>,
    pub vertical_schema: bool,
    pub class: Option<i32>,
}

#[async_trait]
#[allow(dead_code)]
pub trait CdApi: Send + Sync {
    async fn search_locations(
        &self,
        mask: &str,
        location_type: Option<i32>,
        max_count: i32,
    ) -> Result<(Locations, Value), CdError>;
    async fn get_location(
        &self,
        location_type: i32,
        key: i32,
    ) -> Result<(Location, Value), CdError>;
    async fn location_constants(&self, location_type: i32) -> Result<(Locations, Value), CdError>;
    async fn passenger_types(&self) -> Result<(PassengerMapData, Value), CdError>;
    async fn set_connections(
        &self,
        description: &str,
        booking: &BookingRequest,
    ) -> Result<(SearchConnectionInfo, Value), CdError>;
    async fn search_connections(
        &self,
        request: &SearchConnectionsRequest,
    ) -> Result<(SearchConnectionInfo, Value), CdError>;
    async fn connections_page(
        &self,
        handle: i32,
        request: &PageRequest,
    ) -> Result<(ConnectionListInfo, Value), CdError>;
    async fn connection_detail(
        &self,
        handle: i32,
        conn_id: i32,
        flags: Option<i32>,
        booking: &BookingRequest,
    ) -> Result<(ConnectionInfo, Value), CdError>;
    async fn create_price_offer(
        &self,
        handle: i32,
        conn_id: i32,
        flags: Option<i32>,
        booking: &BookingRequest,
    ) -> Result<(PriceOffersInfo, Value), CdError>;
    async fn refresh_price_offer(
        &self,
        booking_id: &str,
        flags: Option<i32>,
    ) -> Result<(PriceOffersInfo, Value), CdError>;
    async fn select_price_offer(
        &self,
        booking_id: &str,
        offer_type: Option<i32>,
    ) -> Result<(PriceOffersInfo, Value), CdError>;
    async fn release_price_offer(&self, booking_id: &str) -> Result<(), CdError>;
    async fn price_offer_info(
        &self,
        booking_id: &str,
    ) -> Result<(FixedPriceOfferInfo, Value), CdError>;
    async fn fix_price_offer(
        &self,
        booking_id: &str,
        customer: &CustomerInfo,
    ) -> Result<(FixedPriceOfferInfo, Value), CdError>;
    async fn reservations(&self, booking_id: &str) -> Result<(ReservationsInfo, Value), CdError>;
    async fn set_reservations(
        &self,
        booking_id: &str,
        reservation: &Value,
    ) -> Result<(ReservationsInfo, Value), CdError>;
    async fn bike_price(
        &self,
        booking_id: &str,
        count: i32,
        bikes: &Value,
    ) -> Result<(BikeRoutePrice, Value), CdError>;
    async fn set_bikes(
        &self,
        booking_id: &str,
        count: i32,
        bikes: &Value,
    ) -> Result<(AvailableAdditionalServices, Value), CdError>;
    async fn dog_price(
        &self,
        booking_id: &str,
        count: i32,
        direction: i32,
    ) -> Result<(LuggageDogPrice, Value), CdError>;
    async fn set_dogs(
        &self,
        booking_id: &str,
        count: i32,
        direction: i32,
    ) -> Result<(AvailableAdditionalServices, Value), CdError>;
    async fn additional_services(
        &self,
        booking_id: &str,
    ) -> Result<(AvailableAdditionalServices, Value), CdError>;
    async fn coach_schema(
        &self,
        request: &SchemaRequest,
    ) -> Result<(CoachesSchema, Value), CdError>;
    async fn sell_tickets(
        &self,
        booking_id: &str,
        partner_booking_id: Option<&str>,
        partner_market: Option<&str>,
    ) -> Result<(SellInfo, Value), CdError>;
    async fn sold_tickets(&self, booking_id: &str) -> Result<(SellInfo, Value), CdError>;
    async fn document(&self, document_id: &str) -> Result<DocumentData, CdError>;
    async fn refund_quote(
        &self,
        ticket_id: &str,
        email: &str,
    ) -> Result<(RefundResult, Value), CdError>;
    async fn refund_ticket(
        &self,
        ticket_id: &str,
        email: &str,
    ) -> Result<Option<(RefundResult, Value)>, CdError>;
    async fn refund_status(&self, ticket_id: &str) -> Result<(RefundsInfos, Value), CdError>;
    async fn refund_changes(
        &self,
        date_time: Option<&str>,
        last_id: Option<i32>,
    ) -> Result<(RefundsInfos, Value), CdError>;
    async fn release_one_ticket_reservation(&self, ticket_id: &str) -> Result<(), CdError>;
}

#[derive(Debug, Clone)]
pub struct DocumentData {
    pub content_type: String,
    pub bytes: Vec<u8>,
}

#[derive(Clone)]
pub struct HttpCdClient {
    client: reqwest::Client,
    config: Arc<CdConfig>,
    signing_key: Arc<RsaPrivateKey>,
}

impl HttpCdClient {
    pub fn new(config: CdConfig) -> Result<Self, CdError> {
        let key_text = config.private_key_pem.expose();
        let signing_key = RsaPrivateKey::from_pkcs8_pem(key_text)
            .or_else(|_| RsaPrivateKey::from_pkcs1_pem(key_text))
            .map_err(|_| CdError::InvalidSigningKey)?;
        let client = reqwest::Client::builder()
            .timeout(config.timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| CdError::Unavailable)?;
        Ok(Self {
            client,
            config: Arc::new(config),
            signing_key: Arc::new(signing_key),
        })
    }

    fn headers(&self, signature: Option<&str>) -> Result<HeaderMap, CdError> {
        let mut headers = HeaderMap::new();
        headers.insert(
            X_USER,
            HeaderValue::from_str(self.config.partner_user.expose())
                .map_err(|_| CdError::NotConfigured)?,
        );
        headers.insert(
            X_DESC,
            HeaderValue::from_str(&self.config.description).map_err(|_| CdError::NotConfigured)?,
        );
        headers.insert(
            X_LANG,
            HeaderValue::from_static(self.config.language.as_str()),
        );
        if let Some(value) = signature {
            headers.insert(
                X_HASH,
                HeaderValue::from_str(value).map_err(|_| CdError::InvalidSigningKey)?,
            );
        }
        Ok(headers)
    }

    pub fn signature(&self, payload: &str) -> String {
        let key = SigningKey::<Sha1>::new((*self.signing_key).clone());
        let base64 = STANDARD.encode(key.sign(payload.as_bytes()).to_bytes());
        utf8_percent_encode(&base64, NON_ALPHANUMERIC).to_string()
    }

    async fn json<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        query: Vec<(&str, String)>,
        body: Option<Value>,
        signature: Option<String>,
    ) -> Result<(T, Value), CdError> {
        let url = format!(
            "{}/{}",
            self.config.base_url.trim_end_matches('/'),
            path.trim_start_matches('/')
        );
        let mut request = self
            .client
            .request(method, url)
            .headers(self.headers(signature.as_deref())?)
            .query(&query);
        if let Some(body) = body {
            request = request.json(&body);
        }
        let response = request.send().await.map_err(map_transport_error)?;
        let status = response.status();
        let bytes = response.bytes().await.map_err(map_transport_error)?;
        if !status.is_success() {
            return Err(parse_error(status, &bytes));
        }
        let value: Value =
            serde_json::from_slice(&bytes).map_err(|_| CdError::MalformedResponse)?;
        let typed =
            serde_json::from_value(value.clone()).map_err(|_| CdError::MalformedResponse)?;
        Ok((typed, value))
    }

    async fn empty(
        &self,
        method: Method,
        path: &str,
        query: Vec<(&str, String)>,
        signature: Option<String>,
    ) -> Result<(), CdError> {
        let url = format!(
            "{}/{}",
            self.config.base_url.trim_end_matches('/'),
            path.trim_start_matches('/')
        );
        let response = self
            .client
            .request(method, url)
            .headers(self.headers(signature.as_deref())?)
            .query(&query)
            .send()
            .await
            .map_err(map_transport_error)?;
        if response.status().is_success() {
            Ok(())
        } else {
            let status = response.status();
            let bytes = response.bytes().await.map_err(map_transport_error)?;
            Err(parse_error(status, &bytes))
        }
    }
}

fn map_transport_error(error: reqwest::Error) -> CdError {
    if error.is_timeout() {
        CdError::Timeout
    } else {
        CdError::Unavailable
    }
}

fn parse_error(status: StatusCode, bytes: &[u8]) -> CdError {
    let parsed: Value = serde_json::from_slice(bytes).unwrap_or(Value::Null);
    let code = parsed
        .get("exceptionCode")
        .and_then(Value::as_i64)
        .and_then(|v| i32::try_from(v).ok());
    let diagnostic = parsed
        .get("exceptionMessage")
        .and_then(Value::as_str)
        .map(sanitize_diagnostic);
    CdError::Rejected {
        status: status.as_u16(),
        upstream_code: code,
        diagnostic,
    }
}

fn sanitize_diagnostic(value: &str) -> String {
    value
        .chars()
        .filter(|c| !c.is_control())
        .take(240)
        .collect()
}

fn qopt<'a, T: ToString>(query: &mut Vec<(&'a str, String)>, key: &'a str, value: Option<T>) {
    if let Some(value) = value {
        query.push((key, value.to_string()));
    }
}

#[async_trait]
impl CdApi for HttpCdClient {
    async fn search_locations(
        &self,
        mask: &str,
        location_type: Option<i32>,
        max_count: i32,
    ) -> Result<(Locations, Value), CdError> {
        let path = location_type.map_or_else(|| "locations".into(), |t| format!("locations/{t}"));
        self.json(
            Method::GET,
            &path,
            vec![("name", mask.into()), ("maxCount", max_count.to_string())],
            None,
            None,
        )
        .await
    }
    async fn get_location(&self, t: i32, key: i32) -> Result<(Location, Value), CdError> {
        self.json(
            Method::GET,
            &format!("locations/{t}/{key}"),
            vec![],
            None,
            None,
        )
        .await
    }
    async fn location_constants(&self, t: i32) -> Result<(Locations, Value), CdError> {
        self.json(
            Method::GET,
            &format!("consts/locations/{t}"),
            vec![],
            None,
            None,
        )
        .await
    }
    async fn passenger_types(&self) -> Result<(PassengerMapData, Value), CdError> {
        self.json(Method::GET, "consts/passengers", vec![], None, None)
            .await
    }
    async fn set_connections(
        &self,
        desc: &str,
        b: &BookingRequest,
    ) -> Result<(SearchConnectionInfo, Value), CdError> {
        self.json(
            Method::POST,
            "connections/set",
            vec![("desc", desc.into())],
            Some(json!({"bookingRequest":b})),
            None,
        )
        .await
    }
    async fn search_connections(
        &self,
        r: &SearchConnectionsRequest,
    ) -> Result<(SearchConnectionInfo, Value), CdError> {
        let mut q = vec![
            ("fromType", r.from_type.to_string()),
            ("from", r.from.to_string()),
            ("toType", r.to_type.to_string()),
            ("to", r.to.to_string()),
            ("prev", r.previous.to_string()),
            ("maxCount", r.max_count.to_string()),
        ];
        qopt(&mut q, "viaType", r.via_type);
        qopt(&mut q, "via", r.via);
        qopt(&mut q, "changeType", r.change_type);
        qopt(&mut q, "change", r.change);
        qopt(&mut q, "dateTime", r.date_time.as_deref());
        self.json(
            Method::POST,
            "connections/search",
            q,
            Some(json!({"connParms":r.parameters,"bookingRequest":r.booking})),
            None,
        )
        .await
    }
    async fn connections_page(
        &self,
        h: i32,
        r: &PageRequest,
    ) -> Result<(ConnectionListInfo, Value), CdError> {
        let mut q = vec![
            ("prev", r.previous.to_string()),
            ("listedCount", r.listed_count.to_string()),
            ("maxCount", r.max_count.to_string()),
        ];
        qopt(&mut q, "connId", r.conn_id);
        self.json(
            Method::POST,
            &format!("connections/{h}"),
            q,
            Some(json!({"bookingRequest":r.booking})),
            None,
        )
        .await
    }
    async fn connection_detail(
        &self,
        h: i32,
        c: i32,
        f: Option<i32>,
        b: &BookingRequest,
    ) -> Result<(ConnectionInfo, Value), CdError> {
        let mut q = vec![];
        qopt(&mut q, "flags", f);
        self.json(
            Method::POST,
            &format!("connections/{h}/{c}"),
            q,
            Some(json!({"bookingRequest":b})),
            None,
        )
        .await
    }
    async fn create_price_offer(
        &self,
        h: i32,
        c: i32,
        f: Option<i32>,
        b: &BookingRequest,
    ) -> Result<(PriceOffersInfo, Value), CdError> {
        let mut q = vec![("handle", h.to_string()), ("connId", c.to_string())];
        qopt(&mut q, "flags", f);
        self.json(
            Method::POST,
            "tickets",
            q,
            Some(json!({"bookingRequest":b})),
            None,
        )
        .await
    }
    async fn refresh_price_offer(
        &self,
        id: &str,
        f: Option<i32>,
    ) -> Result<(PriceOffersInfo, Value), CdError> {
        let mut q = vec![];
        qopt(&mut q, "flags", f);
        self.json(Method::GET, &format!("tickets/{id}"), q, None, None)
            .await
    }
    async fn select_price_offer(
        &self,
        id: &str,
        t: Option<i32>,
    ) -> Result<(PriceOffersInfo, Value), CdError> {
        let mut q = vec![];
        qopt(&mut q, "offerType", t);
        self.json(Method::PUT, &format!("tickets/{id}"), q, None, None)
            .await
    }
    async fn release_price_offer(&self, id: &str) -> Result<(), CdError> {
        self.empty(Method::DELETE, &format!("tickets/{id}"), vec![], None)
            .await
    }
    async fn price_offer_info(&self, id: &str) -> Result<(FixedPriceOfferInfo, Value), CdError> {
        self.json(
            Method::GET,
            &format!("tickets/{id}/info"),
            vec![],
            None,
            None,
        )
        .await
    }
    async fn fix_price_offer(
        &self,
        id: &str,
        c: &CustomerInfo,
    ) -> Result<(FixedPriceOfferInfo, Value), CdError> {
        let mut q = vec![("email", c.email.clone())];
        qopt(&mut q, "name", c.name.as_deref());
        qopt(&mut q, "inCardNumber", c.in_card_number);
        qopt(&mut q, "birthDate", c.birth_date.as_deref());
        qopt(&mut q, "companyName", c.company_name.as_deref());
        self.json(Method::PUT, &format!("tickets/{id}/book"), q, None, None)
            .await
    }
    async fn reservations(&self, id: &str) -> Result<(ReservationsInfo, Value), CdError> {
        self.json(
            Method::GET,
            &format!("reservations/{id}"),
            vec![],
            None,
            None,
        )
        .await
    }
    async fn set_reservations(
        &self,
        id: &str,
        r: &Value,
    ) -> Result<(ReservationsInfo, Value), CdError> {
        self.json(
            Method::PUT,
            &format!("reservations/{id}"),
            vec![],
            Some(json!({"reservationInfo":r})),
            None,
        )
        .await
    }
    async fn bike_price(
        &self,
        id: &str,
        c: i32,
        b: &Value,
    ) -> Result<(BikeRoutePrice, Value), CdError> {
        self.json(
            Method::POST,
            &format!("bikeprice/{id}"),
            vec![("count", c.to_string())],
            Some(json!({"bikesInfo":b})),
            None,
        )
        .await
    }
    async fn set_bikes(
        &self,
        id: &str,
        c: i32,
        b: &Value,
    ) -> Result<(AvailableAdditionalServices, Value), CdError> {
        self.json(
            Method::PUT,
            &format!("bikes/{id}"),
            vec![("count", c.to_string())],
            Some(json!({"bikesInfo":b})),
            None,
        )
        .await
    }
    async fn dog_price(
        &self,
        id: &str,
        c: i32,
        d: i32,
    ) -> Result<(LuggageDogPrice, Value), CdError> {
        self.json(
            Method::GET,
            &format!("dogsprice/{id}"),
            vec![("count", c.to_string()), ("direction", d.to_string())],
            None,
            None,
        )
        .await
    }
    async fn set_dogs(
        &self,
        id: &str,
        c: i32,
        d: i32,
    ) -> Result<(AvailableAdditionalServices, Value), CdError> {
        self.json(
            Method::PUT,
            &format!("dogs/{id}"),
            vec![("count", c.to_string()), ("direction", d.to_string())],
            None,
            None,
        )
        .await
    }
    async fn additional_services(
        &self,
        id: &str,
    ) -> Result<(AvailableAdditionalServices, Value), CdError> {
        self.json(
            Method::GET,
            &format!("addservices/{id}"),
            vec![],
            None,
            None,
        )
        .await
    }
    async fn coach_schema(&self, r: &SchemaRequest) -> Result<(CoachesSchema, Value), CdError> {
        self.json(
            Method::POST,
            "schemas",
            vec![],
            Some(json!({"schemaRequest":r})),
            None,
        )
        .await
    }
    async fn sell_tickets(
        &self,
        id: &str,
        p: Option<&str>,
        m: Option<&str>,
    ) -> Result<(SellInfo, Value), CdError> {
        let payload = format!("payments|{id}|{}|{}", p.unwrap_or(""), m.unwrap_or(""));
        let mut q = vec![];
        qopt(&mut q, "partnerBookingId", p);
        qopt(&mut q, "partnerMarket", m);
        self.json(
            Method::PUT,
            &format!("payments/{id}"),
            q,
            None,
            Some(self.signature(&payload)),
        )
        .await
    }
    async fn sold_tickets(&self, id: &str) -> Result<(SellInfo, Value), CdError> {
        self.json(Method::GET, &format!("payments/{id}"), vec![], None, None)
            .await
    }
    async fn document(&self, id: &str) -> Result<DocumentData, CdError> {
        let encoded_id = utf8_percent_encode(id, NON_ALPHANUMERIC);
        let url = format!(
            "{}/documents/{encoded_id}",
            self.config.base_url.trim_end_matches('/')
        );
        let response = self
            .client
            .get(url)
            .headers(self.headers(None)?)
            .send()
            .await
            .map_err(map_transport_error)?;
        let status = response.status();
        if !status.is_success() {
            let b = response.bytes().await.map_err(map_transport_error)?;
            return Err(parse_error(status, &b));
        }
        let content_type = response
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        if content_type != "application/pdf" && content_type != "image/png" {
            return Err(CdError::MalformedResponse);
        }
        if response
            .content_length()
            .is_some_and(|length| length > MAX_DOCUMENT_BYTES)
        {
            return Err(CdError::MalformedResponse);
        }
        let bytes = response
            .bytes()
            .await
            .map_err(map_transport_error)?
            .to_vec();
        if bytes.len() as u64 > MAX_DOCUMENT_BYTES {
            return Err(CdError::MalformedResponse);
        }
        Ok(DocumentData {
            content_type,
            bytes,
        })
    }
    async fn refund_quote(&self, id: &str, e: &str) -> Result<(RefundResult, Value), CdError> {
        self.json(
            Method::GET,
            &format!("refundpossible/{id}"),
            vec![("email", e.into())],
            None,
            None,
        )
        .await
    }
    async fn refund_ticket(
        &self,
        id: &str,
        e: &str,
    ) -> Result<Option<(RefundResult, Value)>, CdError> {
        let sig = self.signature(&format!("refunds|{id}|{e}"));
        let url = format!(
            "{}/refunds/{id}",
            self.config.base_url.trim_end_matches('/')
        );
        let response = self
            .client
            .post(url)
            .headers(self.headers(Some(&sig))?)
            .query(&[("email", e)])
            .send()
            .await
            .map_err(map_transport_error)?;
        let status = response.status();
        let b = response.bytes().await.map_err(map_transport_error)?;
        if status.is_success() {
            if b.is_empty() {
                return Ok(None);
            }
            let v: Value = serde_json::from_slice(&b).map_err(|_| CdError::MalformedResponse)?;
            let t = serde_json::from_value(v.clone()).map_err(|_| CdError::MalformedResponse)?;
            return Ok(Some((t, v)));
        }
        Err(parse_error(status, &b))
    }
    async fn refund_status(&self, id: &str) -> Result<(RefundsInfos, Value), CdError> {
        self.json(Method::GET, &format!("refunds/{id}"), vec![], None, None)
            .await
    }
    async fn refund_changes(
        &self,
        d: Option<&str>,
        l: Option<i32>,
    ) -> Result<(RefundsInfos, Value), CdError> {
        let mut q = vec![];
        qopt(&mut q, "dateTime", d);
        qopt(&mut q, "lastId", l);
        self.json(Method::GET, "refunds", q, None, None).await
    }
    async fn release_one_ticket_reservation(&self, id: &str) -> Result<(), CdError> {
        let sig = self.signature(&format!("releaseres|{id}"));
        self.empty(Method::POST, &format!("releaseres/{id}"), vec![], Some(sig))
            .await
    }
}

#[allow(dead_code)]
pub fn redact_json(value: &mut Value) {
    const SENSITIVE: &[&str] = &[
        "x-user",
        "x-hash",
        "authorization",
        "privatekey",
        "password",
        "token",
        "email",
        "name",
        "birthdate",
        "incardnumber",
        "documentdata",
        "imgdata",
        "signature",
    ];
    match value {
        Value::Object(map) => {
            for (key, item) in map {
                if SENSITIVE.iter().any(|s| {
                    key.to_ascii_lowercase()
                        .replace(['_', '-'], "")
                        .contains(&s.replace('-', ""))
                }) {
                    *item = Value::String("[REDACTED]".into());
                } else {
                    redact_json(item);
                }
            }
        }
        Value::Array(items) => {
            for item in items {
                redact_json(item);
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        Json, Router,
        body::Body,
        extract::{Request, State},
        http::{Response, header},
        routing::any,
    };
    use rsa::{pkcs8::EncodePrivateKey, rand_core::OsRng};
    use tokio::sync::Mutex;

    #[test]
    fn parses_location_and_preserves_unknown_fields() {
        let parsed: Locations = serde_json::from_value(
            json!({"data":[{"name":"Praha hl.n.","type":3,"key":5457076,"futureField":true}]}),
        )
        .unwrap();
        assert_eq!(parsed.data[0].key, Some(5457076));
        assert_eq!(parsed.data[0].extra["futureField"], true);
    }

    #[test]
    fn rsa_sha1_signatures_verify_and_are_base64() {
        use rsa::signature::Verifier;
        let private = RsaPrivateKey::new(&mut OsRng, 2048).unwrap();
        let pem = private
            .to_pkcs8_pem(Default::default())
            .unwrap()
            .to_string();
        let client = HttpCdClient::new(CdConfig {
            base_url: "http://127.0.0.1".into(),
            partner_user: Secret::new("partner".into()),
            private_key_pem: Secret::new(pem),
            description: "test".into(),
            language: Language::Cs,
            timeout: Duration::from_secs(1),
        })
        .unwrap();
        let message = "payments|booking|partner|market";
        let encoded = client.signature(message);
        let decoded = percent_encoding::percent_decode_str(&encoded)
            .decode_utf8()
            .unwrap();
        let signature = STANDARD.decode(decoded.as_bytes()).unwrap();
        let verifier = rsa::pkcs1v15::VerifyingKey::<Sha1>::new(private.to_public_key());
        verifier
            .verify(
                message.as_bytes(),
                &rsa::pkcs1v15::Signature::try_from(signature.as_slice()).unwrap(),
            )
            .unwrap();
    }

    #[test]
    fn redaction_removes_credentials_and_personal_data() {
        let mut value =
            json!({"X-User":"secret","email":"a@b.cz","nested":{"imgData":"png","safe":"yes"}});
        redact_json(&mut value);
        assert_eq!(value["X-User"], "[REDACTED]");
        assert_eq!(value["nested"]["safe"], "yes");
    }

    #[derive(Clone)]
    struct MockState(Arc<Mutex<Vec<(String, String, bool)>>>);

    async fn upstream_mock(State(state): State<MockState>, request: Request) -> Response<Body> {
        let method = request.method().to_string();
        let path = request.uri().path().to_string();
        let signed = request.headers().contains_key("x-hash");
        assert_eq!(request.headers().get("x-user").unwrap(), "partner");
        state
            .0
            .lock()
            .await
            .push((method.clone(), path.clone(), signed));
        if path.starts_with("/documents/") {
            return Response::builder()
                .status(200)
                .header(header::CONTENT_TYPE, "application/pdf")
                .body(Body::from(b"%PDF-mock".to_vec()))
                .unwrap();
        }
        if method == "DELETE"
            || path.starts_with("/releaseres/")
            || (method == "POST" && path.starts_with("/refunds/"))
        {
            return Response::builder().status(200).body(Body::empty()).unwrap();
        }
        let body = if path == "/locations"
            || path.starts_with("/locations/")
            || path.starts_with("/consts/locations/")
        {
            json!({"data":[]})
        } else if path == "/connections/search" || path == "/connections/set" {
            json!({"handle":1,"connInfo":{"connections":[]}})
        } else if path == "/connections/1" {
            json!({"connections":[]})
        } else {
            json!({})
        };
        Response::builder()
            .status(200)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(body.to_string()))
            .unwrap()
    }

    async fn mock_client() -> (HttpCdClient, Arc<Mutex<Vec<(String, String, bool)>>>) {
        let calls = Arc::new(Mutex::new(Vec::new()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let app = Router::new()
            .fallback(any(upstream_mock))
            .with_state(MockState(calls.clone()));
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let private = RsaPrivateKey::new(&mut OsRng, 2048).unwrap();
        let client = HttpCdClient::new(CdConfig {
            base_url: format!("http://{address}"),
            partner_user: Secret::new("partner".into()),
            private_key_pem: Secret::new(
                private
                    .to_pkcs8_pem(Default::default())
                    .unwrap()
                    .to_string(),
            ),
            description: "test".into(),
            language: Language::Cs,
            timeout: Duration::from_secs(2),
        })
        .unwrap();
        (client, calls)
    }

    #[tokio::test]
    async fn every_documented_operation_uses_the_mock_upstream() {
        let (client, calls) = mock_client().await;
        let booking = BookingRequest::default();
        client.search_locations("pra", None, 10).await.unwrap();
        client.search_locations("pra", Some(3), 10).await.unwrap();
        client.get_location(3, 5457076).await.unwrap();
        client.location_constants(3).await.unwrap();
        client.passenger_types().await.unwrap();
        client
            .set_connections("506,2026-07-05,1,2,10:00,11:00", &booking)
            .await
            .unwrap();
        client
            .search_connections(&SearchConnectionsRequest {
                from_type: 3,
                from: 1,
                to_type: 3,
                to: 2,
                via_type: None,
                via: None,
                change_type: None,
                change: None,
                date_time: None,
                previous: false,
                max_count: 5,
                parameters: ConnectionParameters::default(),
                booking: booking.clone(),
            })
            .await
            .unwrap();
        client
            .connections_page(
                1,
                &PageRequest {
                    conn_id: None,
                    previous: false,
                    listed_count: 0,
                    max_count: 5,
                    booking: booking.clone(),
                },
            )
            .await
            .unwrap();
        client
            .connection_detail(1, 2, None, &booking)
            .await
            .unwrap();
        client
            .create_price_offer(1, 2, None, &booking)
            .await
            .unwrap();
        client.refresh_price_offer("BOOK", None).await.unwrap();
        client.select_price_offer("BOOK", Some(1)).await.unwrap();
        client.release_price_offer("BOOK").await.unwrap();
        client.price_offer_info("BOOK").await.unwrap();
        let customer = CustomerInfo {
            email: "person@example.cz".into(),
            name: None,
            in_card_number: None,
            birth_date: None,
            company_name: None,
        };
        client.fix_price_offer("BOOK", &customer).await.unwrap();
        client.reservations("BOOK").await.unwrap();
        client
            .set_reservations("BOOK", &json!({"trains":[]}))
            .await
            .unwrap();
        client
            .bike_price("BOOK", 1, &json!({"trains":[]}))
            .await
            .unwrap();
        client
            .set_bikes("BOOK", 1, &json!({"trains":[]}))
            .await
            .unwrap();
        client.dog_price("BOOK", 1, 2).await.unwrap();
        client.set_dogs("BOOK", 1, 2).await.unwrap();
        client.additional_services("BOOK").await.unwrap();
        client
            .coach_schema(&SchemaRequest {
                flags: 1,
                schema_info: json!({}),
                coach_number: None,
                self_reserved_seats: vec![],
                schema_width: 800,
                schema_max_height: None,
                vertical_schema: false,
                class: None,
            })
            .await
            .unwrap();
        client
            .sell_tickets("BOOK", Some("PARTNER"), Some("MARKET"))
            .await
            .unwrap();
        client.sold_tickets("BOOK").await.unwrap();
        client.document("DOC").await.unwrap();
        client
            .refund_quote("TICKET", "person@example.cz")
            .await
            .unwrap();
        client
            .refund_ticket("TICKET", "person@example.cz")
            .await
            .unwrap();
        client.refund_status("TICKET").await.unwrap();
        client.refund_changes(None, Some(1)).await.unwrap();
        client
            .release_one_ticket_reservation("CD12345.abcdef")
            .await
            .unwrap();

        let calls = calls.lock().await;
        assert_eq!(calls.len(), 31);
        assert_eq!(calls.iter().filter(|(_, _, signed)| *signed).count(), 3);
        assert!(
            calls
                .iter()
                .all(|(_, path, _)| !path.contains("ticket-api.cd.cz"))
        );
    }

    #[test]
    fn upstream_errors_are_sanitized_and_stably_mapped() {
        let error = parse_error(
            StatusCode::NOT_FOUND,
            br#"{"exceptionCode":42,"exceptionMessage":"expired\nhandle"}"#,
        );
        assert_eq!(error.stable_code(), "upstream_resource_expired");
        match error {
            CdError::Rejected {
                upstream_code,
                diagnostic,
                ..
            } => {
                assert_eq!(upstream_code, Some(42));
                assert_eq!(diagnostic.as_deref(), Some("expiredhandle"));
            }
            _ => panic!("wrong error"),
        }
    }

    async fn client_for_app(app: Router, timeout: Duration) -> HttpCdClient {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let private = RsaPrivateKey::new(&mut OsRng, 2048).unwrap();
        HttpCdClient::new(CdConfig {
            base_url: format!("http://{address}"),
            partner_user: Secret::new("partner".into()),
            private_key_pem: Secret::new(
                private
                    .to_pkcs8_pem(Default::default())
                    .unwrap()
                    .to_string(),
            ),
            description: "test".into(),
            language: Language::Cs,
            timeout,
        })
        .unwrap()
    }

    #[tokio::test]
    async fn malformed_upstream_json_is_rejected() {
        async fn malformed() -> &'static str {
            "not-json"
        }
        let client = client_for_app(
            Router::new().fallback(any(malformed)),
            Duration::from_secs(1),
        )
        .await;
        assert!(matches!(
            client.passenger_types().await,
            Err(CdError::MalformedResponse)
        ));
    }

    #[tokio::test]
    async fn upstream_timeouts_have_a_stable_error() {
        async fn slow() -> Json<Value> {
            tokio::time::sleep(Duration::from_millis(100)).await;
            Json(json!({}))
        }
        let client =
            client_for_app(Router::new().fallback(any(slow)), Duration::from_millis(10)).await;
        assert!(matches!(
            client.passenger_types().await,
            Err(CdError::Timeout)
        ));
    }
}
