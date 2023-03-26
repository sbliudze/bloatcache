package edu.kit.kastel.formal.bloatcache;

import org.junit.jupiter.api.Test;

import static edu.kit.kastel.formal.bloatcache.Util.checkArgument;
import static edu.kit.kastel.formal.bloatcache.Util.checkArguments;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author Alexander Weigl
 * @version 1 (19.03.23)
 */
class UtilTest {

    @Test
    void testCheckArguments() {
        checkArguments(args, "set", "K", "F", "T", "I");
    }

    @Test
    void testCheckArgument() {
        assertTrue(checkArgument("abc", "abc"));
        assertTrue(checkArgument("set", "set"));
        assertTrue(checkArgument("move", "move"));
        assertTrue(checkArgument("add", "add"));

        assertTrue(checkArgument("add", "add|set"));
        assertTrue(checkArgument("set", "add|set"));

        assertFalse(checkArgument("move", "add|set"));


        assertTrue(checkArgument("THISISAVALIDKEY!", "K"));
        assertTrue(checkArgument("THIS_IS_ALSO_VALID&!/!!%!%§$!&!!", "K"));

        assertFalse(checkArgument("THIS_KEY_IS_TOO_LONG_________________________________________________________" +
                "____________________________________________________________________________________________________" +
                "____________________________________________________________________________________________________" +
                "_________________________________________________________________________________________________-___" +
                "________________________________________________________________", "K"));
        assertFalse(checkArgument("THIS KEY IS NOT VALID", "K"));

        assertTrue(checkArgument("2242424", "I"));
        assertFalse(checkArgument("a24324324", "I"));
        assertTrue(checkArgument("noreply", "[noreply]"));
        assertTrue(checkArgument(null, "[noreply]"));
    }
}