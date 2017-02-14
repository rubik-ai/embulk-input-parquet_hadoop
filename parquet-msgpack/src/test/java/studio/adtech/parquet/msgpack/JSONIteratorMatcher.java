package studio.adtech.parquet.msgpack;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeDiagnosingMatcher;
import org.hamcrest.TypeSafeMatcher;
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
                    JSONCompareResult r = JSONCompare.compareJSON(left, right, JSONCompareMode.STRICT_ORDER);
                    if (r.failed()) {
                        mismatchDescription.appendText(r.getMessage());
                        return false;
                    }
                } catch (JSONException e) {
                    mismatchDescription.appendText(e.toString());
                    throw new AssertionError(e);
                }
            } else if (!hasLeft && !hasRight) {
                return true;
            } else {
                // left or right has extra line
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
