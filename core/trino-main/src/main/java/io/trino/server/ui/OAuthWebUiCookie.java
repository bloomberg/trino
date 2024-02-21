/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.server.ui;

import jakarta.ws.rs.core.Cookie;
import jakarta.ws.rs.core.NewCookie;

import java.time.Instant;
import java.util.Date;
import java.util.Map;
import java.util.Optional;

import static io.trino.server.ui.FormWebUiAuthenticationFilter.UI_LOCATION;
import static java.util.function.Predicate.not;

public final class OAuthWebUiCookie
{
    // prefix according to: https://tools.ietf.org/html/draft-ietf-httpbis-rfc6265bis-05#section-4.1.3.1
    public static final String OAUTH2_COOKIE = "__Secure-Trino-OAuth2-Token";

    private OAuthWebUiCookie() {}

    public static NewCookie[] create(String token, Instant tokenExpiration)
    {
        return new NewCookie[] {new NewCookie.Builder(OAUTH2_COOKIE)
                .path(UI_LOCATION)
                .domain(null)
                .value(token)
                .expiry(Date.from(tokenExpiration))
                .secure(true)
                .httpOnly(true)
                .build()};
    }

    public static Optional<String> read(Cookie cookie)
    {
        return Optional.ofNullable(cookie)
                .map(Cookie::getValue)
                .filter(not(String::isBlank));
    }

    public static NewCookie[] delete(Map<String, Cookie> availableCookies)
    {
        return new NewCookie[] {new NewCookie.Builder(OAUTH2_COOKIE)
                .value("delete")
                .path(UI_LOCATION)
                .domain(null)
                .maxAge(0)
                .expiry(null)
                .secure(true)
                .httpOnly(true)
                .build()};
    }
}
