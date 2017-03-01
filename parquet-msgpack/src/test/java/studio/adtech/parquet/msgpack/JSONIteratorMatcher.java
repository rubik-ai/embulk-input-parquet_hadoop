/*
 * Copyright 2017 CyberAgent, Inc.
 *
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
package studio.adtech.parquet.msgpack;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.json.JSONException;
import org.skyscreamer.jsonassert.JSONCompare;
import org.skyscreamer.jsonassert.JSONCompareMode;
import org.skyscreamer.jsonassert.JSONCompareResult;

import java.util.Iterator;

public class JSONIteratorMatcher extends TypeSafeDiagnosingMatcher<JSONIterator> {
    private final JSONIterator expected;

    private JSONIteratorMatcher(JSONIterator expected) {
        this.expected = expected;
    }

    @Override
    protected boolean matchesSafely(JSONIterator actual, Description mismatchDescription) {
        int line = 1;

        Iterator<String> itLeft = expected;
        Iterator<String> itRight = actual;

        while (true) {
            boolean hasLeft = itLeft.hasNext();
            boolean hasRight = itRight.hasNext();

            if (hasLeft && hasRight) {
                String left = itLeft.next();
                String right = itRight.next();

                try {
                    JSONCompareResult r = JSONCompare.compareJSON(left, right, JSONCompareMode.STRICT);
                    if (r.failed()) {
                        mismatchDescription.appendText("at line " + line + ": ");
                        mismatchDescription.appendText(r.getMessage());
                        return false;
                    }
                } catch (JSONException e) {
                    mismatchDescription.appendText("at line " + line + ": ");
                    mismatchDescription.appendText(e.toString());
                    throw new AssertionError(e);
                }
            } else if (!hasLeft && !hasRight) {
                return true;
            } else {
                // left or right has extra line
                mismatchDescription.appendText("at line " + line + ": ");
                if (hasLeft) {
                    mismatchDescription.appendText("expected has extra lines");
                } else {
                    mismatchDescription.appendText("actual has extra lines");
                }
                return false;
            }

            line += 1;
        }
    }

    public static Matcher<JSONIterator> sameAs(JSONIterator expected) {
        return new JSONIteratorMatcher(expected);
    }

    @Override
    public void describeTo(Description description) {

    }
}
