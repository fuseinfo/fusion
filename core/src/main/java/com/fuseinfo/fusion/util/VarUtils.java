/*
 * Copyright (c) 2018 Fuseinfo Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package com.fuseinfo.fusion.util;

import scala.Function1;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class VarUtils {
    private final static ConcurrentHashMap<String, Function1<String, String>> cmdMap = new ConcurrentHashMap<>();

    public static String enrichString(String str, Map<String, String> vars) {
        StringBuilder sb = new StringBuilder();
        int next = 0;
        for (int i = 0; i < str.length(); i++) {
            if (str.charAt(i) == '\\' && i < str.length() - 1) {
                sb.append(str, next, i).append(str.charAt(++i));
                next = i + 1;
                continue;
            }

            if (str.charAt(i) == '$' && i < str.length() - 1) {
                if (str.charAt(i + 1) == '(') {
                    sb.append(str, next, i);
                    next = processCommand(str, i + 2, sb, vars);
                    i = next - 1;
                } else if (str.charAt(i + 1) == '{') {
                    sb.append(str, next, i);
                    next = processVar(str, i + 2, sb, vars);
                    i = next - 1;
                }
            }
        }
        if (next == 0) return str;
        if (next < str.length()) sb.append(str, next, str.length());
        return sb.toString();
    }

    private static int processCommand(String str, int pos, StringBuilder sb, Map<String, String> vars) {
        StringBuilder cmdSB = sb == null?null:new StringBuilder();
        int next = pos;
        for (int i = pos; i < str.length(); i++) {
            if (str.charAt(i) == '\\' && i < str.length() - 1) {
                cmdSB.append(str, next, i).append(str.charAt(++i));
                next = i + 1;
                continue;
            }
            if (str.charAt(i) == ')') {
                if (next < i) append(cmdSB, str, next, i);
                if (sb != null) {
                    sb.append(runCommand(cmdSB.toString()));
                }
                return i + 1;
            }
            if (str.charAt(i) == '$' && i < str.length() - 1) {
                if (str.charAt(i + 1) == '(') {
                    append(cmdSB, str, next, i);
                    next = processCommand(str, i + 2, cmdSB, vars);
                    i = next - 1;
                } else if (str.charAt(i + 1) == '{') {
                    append(cmdSB, str, next, i);
                    next = processVar(str, i + 2, cmdSB, vars);
                    i = next - 1;
                }
            }
        }
        return str.length();
    }

    private static void append(StringBuilder sb, CharSequence cs) {
        if (sb != null) sb.append(cs);
    }

    private static void append(StringBuilder sb, CharSequence cs, int pos, int end) {
        if (sb != null) sb.append(cs, pos, end);
    }

    private static int processVar(String str, int pos, StringBuilder sb, Map<String, String> vars) {
        for (int i = pos; i < str.length(); i++) {
            if (str.charAt(i) == '}') {
                append(sb, vars.getOrDefault(str.substring(pos, i), ""));
                return i + 1;
            } else if (str.charAt(i) == ':' && i < str.length() - 1 && str.charAt(i + 1) == '-') {
                String varName = str.substring(pos, i);
                StringBuilder varSB;
                if (vars.containsKey(varName) || sb == null) {
                    append(sb, vars.getOrDefault(varName, ""));
                    varSB = null;
                } else varSB = new StringBuilder();
                int next = i + 2;
                for (i = next; i < str.length(); i++) {
                    if (str.charAt(i) == '\\' && i < str.length() - 1) {
                        varSB.append(str, next, i).append(str.charAt(++i));
                        next = i + 1;
                        continue;
                    }
                    if (str.charAt(i) == '}') {
                        if (next < i ) append(varSB, str, next, i);
                        if (sb != null && varSB != null)
                            sb.append(varSB);
                        return i + 1;
                    }
                    if (str.charAt(i) == '$' && i < str.length() - 1) {
                        if (str.charAt(i + 1) == '(') {
                            append(varSB, str, next, i);
                            next = processCommand(str, i + 2, varSB, vars);
                            i = next - 1;
                        } else if (str.charAt(i + 1) == '{') {
                            append(varSB, str, next, i);
                            next = processVar(str, i + 2, varSB, vars);
                            i = next - 1;
                        }
                    }
                }
            }
        }
        return str.length();
    }

    @SuppressWarnings("unchecked")
    private static String runCommand(String cmdStr) {
        int space = cmdStr.indexOf(' ');
        String cmd, params;
        if (space > 0) {
            cmd = cmdStr.substring(0, space);
            params = cmdStr.substring(space + 1).trim();
        } else {
            cmd = cmdStr;
            params = "";
        }
        Function1<String, String> func = cmdMap.get(cmd);
        if (func == null) {
            try {
                try {
                    func = (Function1<String, String>)Class.forName(cmd).newInstance();
                } catch(ClassNotFoundException e) {
                    func = (Function1<String, String>)Class.forName("com.fuseinfo.fusion.command." + cmd).newInstance();
                }
                cmdMap.put(cmd, func);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return func.apply(params);
    }

    public final static Map<String, String> EMPTY_MAP = java.util.Collections.unmodifiableMap(new HashMap<>());
}
