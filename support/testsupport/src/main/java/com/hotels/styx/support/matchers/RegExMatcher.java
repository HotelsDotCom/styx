/*
  Copyright (C) 2013-2021 Expedia Inc.

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */
package com.hotels.styx.support.matchers;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

public class RegExMatcher extends TypeSafeMatcher<String> {
    private final String regex;

    public static Matcher<String> matchesRegex(String regex) {
        return new RegExMatcher(regex);
    }

    public RegExMatcher(String regex) {
        this.regex = regex;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("String matching regex: '" + regex + "'");
    }

    @Override
    protected void describeMismatchSafely(String item, Description mismatchDescription) {
        mismatchDescription.appendText(item + " does not match [" + regex + "]");
    }

    @Override
    protected boolean matchesSafely(String text) {
        return text.matches(regex);
    }
}
