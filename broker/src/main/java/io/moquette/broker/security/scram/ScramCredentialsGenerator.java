/*
 * Copyright (c) 2012-2026 The original author or authors
 * ------------------------------------------------------
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * and Apache License v2.0 which accompanies this distribution.
 *
 * The Eclipse Public License is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * The Apache License v2.0 is available at
 * http://www.opensource.org/licenses/apache2.0.php
 *
 * You may elect to redistribute this code under either of these licenses.
 */

package io.moquette.broker.security.scram;

import com.ongres.scram.common.ScramFunctions;
import com.ongres.scram.common.ScramMechanism;
import com.ongres.scram.common.StringPreparation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Utility to generate the data a server has to store to later authenticate a user with SCRAM-SHA-256.
 * The plaintext password is never stored, only the salt, the iteration count, the StoredKey and the ServerKey.
 * */
public final class ScramCredentialsGenerator {

    /**
     * Data to be persisted on the server for a user.
     * */
    public static final class ScramUserData {

        private final String username;
        private final String base64Salt;
        private final int iterationCount;
        private final String base64StoredKey;
        private final String base64ServerKey;

        public ScramUserData(String username, String base64Salt, int iterationCount,
                      String base64StoredKey, String base64ServerKey) {
            this.username = username;
            this.base64Salt = base64Salt;
            this.iterationCount = iterationCount;
            this.base64StoredKey = base64StoredKey;
            this.base64ServerKey = base64ServerKey;
        }

        public String getUsername() {
            return username;
        }

        public String getBase64Salt() {
            return base64Salt;
        }

        public int getIterationCount() {
            return iterationCount;
        }

        public String getBase64StoredKey() {
            return base64StoredKey;
        }

        public String getBase64ServerKey() {
            return base64ServerKey;
        }
    }

    private static final ScramMechanism MECHANISM = ScramMechanism.SCRAM_SHA_256;
    private static final int SALT_LENGTH = 32;
    private static final SecureRandom RANDOM = new SecureRandom();

    private ScramCredentialsGenerator() {
    }

    /**
     * Generates the SCRAM-SHA-256 data to store for the given user.
     * The iteration count is the number of PBKDF2 iterations, RFC 7677 recommends at least 4096.
     * */
    public static ScramUserData registerNewUser(String username, char[] plaintextPassword, int iterationCount) {
        // random salt
        final byte[] salt = ScramFunctions.salt(SALT_LENGTH, RANDOM);

        // SaltedPassword = Hi(Normalize(password), salt, i)
        final byte[] saltedPassword = ScramFunctions.saltedPassword(MECHANISM, StringPreparation.SASL_PREPARATION,
            plaintextPassword, salt, iterationCount);

        final byte[] clientKey = ScramFunctions.clientKey(MECHANISM, saltedPassword);
        final byte[] serverKey = ScramFunctions.serverKey(MECHANISM, saltedPassword);
        // the server stores StoredKey to verify the client without knowing ClientKey
        final byte[] storedKey = ScramFunctions.storedKey(MECHANISM, clientKey);

        final Base64.Encoder encoder = Base64.getEncoder();
        return new ScramUserData(username,
            encoder.encodeToString(salt),
            iterationCount,
            encoder.encodeToString(storedKey),
            encoder.encodeToString(serverKey));
    }

    private static final int DEFAULT_ITERATION_COUNT = 4096;
    private static final String USERS_FILE_NAME = "users.yaml";

    /**
     * Simple CLI to generate the SCRAM-SHA-256 data for a user and store it in config/users.yaml
     * under the Moquette installation folder (system property moquette.path).
     * The typed password is echoed as '*' characters.
     * */
    public static void main(String[] args) throws IOException {
        System.out.println("Moquette SCRAM-SHA-256 credentials generator");
        System.out.println("Type username and password to get the data to store on the server.");
        System.out.println();

        final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
        System.out.print("Username: ");
        System.out.flush();
        final String username = reader.readLine();
        if (username == null || username.isEmpty()) {
            System.err.println("Username must not be empty");
            System.exit(1);
        }

        System.out.print("Password: ");
        System.out.flush();
        final char[] password = readMaskedPassword(reader);
        if (password.length == 0) {
            System.err.println("Password must not be empty");
            System.exit(1);
        }

        final ScramUserData data = registerNewUser(username, password, DEFAULT_ITERATION_COUNT);
        java.util.Arrays.fill(password, '\0');

        final Path usersFile = Paths.get(System.getProperty("moquette.path", "."), "config", USERS_FILE_NAME);
        storeUser(usersFile, data);

        System.out.println();
        System.out.println("User '" + data.getUsername() + "' stored in " + usersFile.toAbsolutePath());
    }

    /**
     * Adds (or replaces) the user in the YAML file, creating the file if it doesn't exist.
     * The file is a list of users with fields username, salt, iterationCount, storedKey, serverKey.
     * */
    public static void storeUser(Path usersFile, ScramUserData data) throws IOException {
        final Map<String, ScramUserData> users = loadUsers(usersFile);
        users.put(data.getUsername(), data);

        Files.createDirectories(usersFile.toAbsolutePath().getParent());
        // write to a temp file and move it, to not leave a truncated file in case of failure
        final Path tmpFile = usersFile.resolveSibling(USERS_FILE_NAME + ".tmp");
        Files.write(tmpFile, toYaml(users).getBytes(StandardCharsets.UTF_8));
        Files.move(tmpFile, usersFile, StandardCopyOption.REPLACE_EXISTING);
    }

    /**
     * Loads the users from the YAML file, indexed by username. If the file doesn't exist or is empty
     * the result is an empty (modifiable) map.
     * */
    static Map<String, ScramUserData> loadUsers(Path usersFile) throws IOException {
        if (Files.exists(usersFile) && Files.size(usersFile) > 0) {
            final String content = new String(Files.readAllBytes(usersFile), StandardCharsets.UTF_8);
            return new UsersYamlParser(content).parse();
        }
        return new LinkedHashMap<>();
    }

    static String toYaml(Map<String, ScramUserData> users) {
        final StringBuilder sb = new StringBuilder();
        for (ScramUserData user : users.values()) {
            sb.append("- username: ").append(quote(user.getUsername())).append('\n');
            sb.append("  salt: ").append(quote(user.getBase64Salt())).append('\n');
            sb.append("  iterationCount: ").append(user.getIterationCount()).append('\n');
            sb.append("  storedKey: ").append(quote(user.getBase64StoredKey())).append('\n');
            sb.append("  serverKey: ").append(quote(user.getBase64ServerKey())).append('\n');
        }
        return sb.toString();
    }

    // YAML double quoted scalar
    private static String quote(String value) {
        final StringBuilder sb = new StringBuilder("\"");
        for (char c : value.toCharArray()) {
            switch (c) {
                case '"':
                    sb.append("\\\"");
                    break;
                case '\\':
                    sb.append("\\\\");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                default:
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        return sb.append('"').toString();
    }

    /**
     * Minimal line based parser for the users file format written by toYaml:
     * <pre>
     * - username: "name"
     *   salt: "..."
     *   iterationCount: 4096
     *   storedKey: "..."
     *   serverKey: "..."
     * </pre>
     * Each list item starts with "- ". Blank lines and comments are ignored, scalars can be plain or double quoted.
     * */
    private static final class UsersYamlParser {
        private final String[] lines;
        private int lineNo = 0;
        // cursor inside the line being parsed
        private String line;
        private int pos;

        UsersYamlParser(String text) {
            this.lines = text.split("\r?\n", -1);
        }

        Map<String, ScramUserData> parse() throws IOException {
            final Map<String, ScramUserData> users = new LinkedHashMap<>();
            boolean inItem = false;
            String username = null;
            String salt = null;
            String storedKey = null;
            String serverKey = null;
            Integer iterationCount = null;

            for (lineNo = 1; lineNo <= lines.length; lineNo++) {
                line = lines[lineNo - 1];
                if (line.trim().isEmpty() || line.trim().startsWith("#")) {
                    continue;
                }
                pos = 0;
                skipSpaces();
                if (line.startsWith("- ", pos) || line.substring(pos).equals("-")) {
                    // a new list item begins, close the previous one
                    if (inItem) {
                        users.put(username, build(username, salt, iterationCount, storedKey, serverKey));
                    }
                    inItem = true;
                    username = null;
                    salt = null;
                    storedKey = null;
                    serverKey = null;
                    iterationCount = null;
                    pos++;
                    skipSpaces();
                    if (pos >= line.length()) {
                        continue;
                    }
                } else if (!inItem) {
                    throw error("expected a list item starting with '- '");
                }
                final String key = readScalar(true);
                expectColon();
                final String value = readScalar(false);
                expectEndOfLine();
                switch (key) {
                    case "username":
                        username = value;
                        break;
                    case "salt":
                        salt = value;
                        break;
                    case "storedKey":
                        storedKey = value;
                        break;
                    case "serverKey":
                        serverKey = value;
                        break;
                    case "iterationCount":
                        try {
                            iterationCount = Integer.valueOf(value);
                        } catch (NumberFormatException e) {
                            throw error("iterationCount is not a number");
                        }
                        break;
                    default:
                        throw error("unknown field " + key);
                }
            }
            if (inItem) {
                users.put(username, build(username, salt, iterationCount, storedKey, serverKey));
            }
            return users;
        }

        private ScramUserData build(String username, String salt, Integer iterationCount,
                                    String storedKey, String serverKey) throws IOException {
            if (username == null || salt == null || storedKey == null || serverKey == null || iterationCount == null) {
                throw new IOException("Invalid users file: a user entry has missing fields (username " + username + ")");
            }
            return new ScramUserData(username, salt, iterationCount, storedKey, serverKey);
        }

        private String readScalar(boolean isKey) throws IOException {
            skipSpaces();
            if (pos < line.length() && line.charAt(pos) == '"') {
                return readQuoted();
            }
            // plain scalar, up to a ':' followed by space/end for keys, or up to a comment for values
            final int start = pos;
            while (pos < line.length()) {
                final char c = line.charAt(pos);
                if (isKey && c == ':' && (pos + 1 == line.length() || Character.isWhitespace(line.charAt(pos + 1)))) {
                    break;
                }
                if (!isKey && c == '#' && pos > start && Character.isWhitespace(line.charAt(pos - 1))) {
                    break;
                }
                pos++;
            }
            final String value = line.substring(start, pos).trim();
            if (value.isEmpty()) {
                throw error("expected a value");
            }
            return value;
        }

        private String readQuoted() throws IOException {
            pos++; // opening quote
            final StringBuilder sb = new StringBuilder();
            while (pos < line.length()) {
                final char c = line.charAt(pos++);
                if (c == '"') {
                    return sb.toString();
                }
                if (c != '\\') {
                    sb.append(c);
                    continue;
                }
                if (pos >= line.length()) {
                    break;
                }
                final char esc = line.charAt(pos++);
                switch (esc) {
                    case 'n':
                        sb.append('\n');
                        break;
                    case 'r':
                        sb.append('\r');
                        break;
                    case 't':
                        sb.append('\t');
                        break;
                    case 'u':
                        if (pos + 4 > line.length()) {
                            throw error("bad unicode escape");
                        }
                        sb.append((char) Integer.parseInt(line.substring(pos, pos + 4), 16));
                        pos += 4;
                        break;
                    default: // covers \" and \\
                        sb.append(esc);
                }
            }
            throw error("unterminated string");
        }

        private void skipSpaces() {
            while (pos < line.length() && Character.isWhitespace(line.charAt(pos))) {
                pos++;
            }
        }

        private void expectColon() throws IOException {
            skipSpaces();
            if (pos >= line.length() || line.charAt(pos) != ':') {
                throw error("expected ':'");
            }
            pos++;
        }

        private void expectEndOfLine() throws IOException {
            skipSpaces();
            if (pos < line.length() && line.charAt(pos) != '#') {
                throw error("unexpected content '" + line.substring(pos) + "'");
            }
        }

        private IOException error(String message) {
            return new IOException("Invalid users file at line " + lineNo + ": " + message);
        }
    }

    /**
     * Reads a line from the terminal printing '*' for each typed character.
     * It disables the terminal echo and canonical mode using stty, so it works on Unix like terminals;
     * if the terminal can't be configured (e.g. run from an IDE) it falls back to a plain visible read.
     * */
    private static char[] readMaskedPassword(BufferedReader reader) throws IOException {
        final String savedTtyState = sttyOutput("-g");
        if (savedTtyState == null || stty("-echo -icanon min 1") != 0) {
            System.err.println("(cannot hide the input, no terminal available, password will be visible)");
            final String line = reader.readLine();
            return line == null ? new char[0] : line.toCharArray();
        }

        final Thread restoreTty = new Thread(() -> stty(savedTtyState));
        Runtime.getRuntime().addShutdownHook(restoreTty);
        final StringBuilder password = new StringBuilder();
        try {
            int c;
            while ((c = reader.read()) != -1 && c != '\n' && c != '\r') {
                if (c == 127 || c == '\b') { // backspace
                    if (password.length() > 0) {
                        password.setLength(password.length() - 1);
                        System.out.print("\b \b");
                    }
                } else {
                    password.append((char) c);
                    System.out.print('*');
                }
                System.out.flush();
            }
        } finally {
            stty(savedTtyState);
            Runtime.getRuntime().removeShutdownHook(restoreTty);
            System.out.println();
        }
        final char[] result = new char[password.length()];
        password.getChars(0, password.length(), result, 0);
        password.setLength(0);
        return result;
    }

    private static int stty(String options) {
        try {
            return new ProcessBuilder("sh", "-c", "stty " + options + " < /dev/tty")
                .redirectErrorStream(true)
                .start()
                .waitFor();
        } catch (IOException e) {
            return -1;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return -1;
        }
    }

    private static String sttyOutput(String options) {
        try {
            final Process process = new ProcessBuilder("sh", "-c", "stty " + options + " < /dev/tty")
                .redirectErrorStream(true)
                .start();
            final String out;
            try (BufferedReader r = new BufferedReader(new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                out = r.readLine();
            }
            return process.waitFor() == 0 ? out : null;
        } catch (IOException e) {
            return null;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        }
    }
}
