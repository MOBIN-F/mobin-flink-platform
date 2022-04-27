package com.mobin.cli;

import com.mobin.MlinkException;
import org.apache.commons.cli.*;
import org.apache.flink.core.fs.Path;

import java.io.File;
import java.net.URL;

/**
 * Created with IDEA
 * Creater: MOBIN
 * Date: 2021/9/10
 * Time: 2:26 PM
 */
public class CliOptionParser {

    public static final Option OPTION_SQL = Option
            .builder("sql")
            .longOpt("sql")
            .required(false)
            .numberOfArgs(1)
            .argName("flink sql file")
            .desc("flink sql file")
            .build();

    public static final Option OPTION_CONNECTORS= Option
            .builder("connectors")
            .longOpt("connectors")
            .required(false)
            .numberOfArgs(1)
            .argName("connectors")
            .desc("connectors")
            .build();


    private static final Options MLINK_CLIENT_OPTIONS = getMlinkClientOptions(new Options());

    public static Options getMlinkClientOptions(Options options){
        options.addOption(OPTION_SQL);
        options.addOption(OPTION_CONNECTORS);
        return options;
    }

    public static CliOptions parseClient(String[] args) {
        try {
            DefaultParser parser = new DefaultParser();
            CommandLine line = parser.parse(MLINK_CLIENT_OPTIONS, args, true);
            CliOptions cliOptions = new CliOptions(
                    checkUrl(line, CliOptionParser.OPTION_SQL),
                    line.getOptionValue(CliOptionParser.OPTION_CONNECTORS.getOpt())
            );
            return cliOptions;
        } catch (ParseException e) {
            throw new MlinkException(e.getMessage());
        }
    }

    private static URL checkUrl(CommandLine line, Option option) {
        final URL url = checkUrls(line,option);
        if (url != null ) {
            return url;
        }
        return null;
    }

    private static URL checkUrls(CommandLine line, Option option) {
        if (line.hasOption(option.getOpt())) {
            final String url = line.getOptionValue(option.getOpt());
            try {
                return Path.fromLocalFile(new File(url).getAbsoluteFile())
                        .toUri()
                        .toURL();
            } catch (Exception e) {
                throw new MlinkException(
                        "Invalid path for option '"
                                + option.getLongOpt()
                                + "': "
                                + url,
                        e);
            }
        }
        return null;
    }

    public static void helpFormatter(){
        HelpFormatter helpFormatter = new HelpFormatter();
        helpFormatter.printHelp(" ", MLINK_CLIENT_OPTIONS);
    }
}
