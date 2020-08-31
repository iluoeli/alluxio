package alluxio.jnifuse.utils;

import alluxio.jnifuse.platform.x86_64.darwin.TypeAliases;
import alluxio.jnifuse.struct.NativeType;
import alluxio.jnifuse.struct.TypeAlias;

import java.util.Map;

public class Platform {
    public static final String OS_NAME = System.getProperty("os.name");
    public static final String PROCESSOR_BIT = System.getProperty("os.arch");
    public static final String OS_NAME_LC;
    public static final boolean IS_LINUX;
    public static final boolean IS_MAC;
    public static final boolean IS_32_BIT;
    public static final boolean IS_64_BIT;

    static {
        OS_NAME_LC = OS_NAME.toLowerCase();
        IS_LINUX = OS_NAME_LC.startsWith("linux");
        IS_MAC = OS_NAME_LC.startsWith("mac os") || OS_NAME_LC.startsWith("darwin");
        IS_32_BIT = PROCESSOR_BIT.contains("32");
        IS_64_BIT = PROCESSOR_BIT.contains("32");
    }

    /** Type aliases which is platform-dependent */
    public static Map<TypeAlias, NativeType> ALIASES = buildTypeAliases();

    public static Map<TypeAlias, NativeType> buildTypeAliases() {
        if (Platform.IS_MAC) {
            return alluxio.jnifuse.platform.x86_64.darwin.TypeAliases.ALIASES;
        } else {
            return alluxio.jnifuse.platform.x86_64.linux.TypeAliases.ALIASES;
        }
    }

    public static NativeType findNativeType(TypeAlias typeAlias) {
        return ALIASES.get(typeAlias);
    }
}
