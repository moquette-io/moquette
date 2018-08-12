package io.moquette.broker;

import io.moquette.BrokerConstants;
import io.moquette.server.config.IConfig;

public class BrokerConfiguration {

    final boolean allowAnonymous;
    final boolean allowZeroByteClientId;
    final boolean reauthorizeSubscriptionsOnConnect;

    public BrokerConfiguration(IConfig props) {
        allowAnonymous = props.boolProp(BrokerConstants.ALLOW_ANONYMOUS_PROPERTY_NAME, true);
        allowZeroByteClientId = props.boolProp(BrokerConstants.ALLOW_ZERO_BYTE_CLIENT_ID_PROPERTY_NAME, false);
        reauthorizeSubscriptionsOnConnect = props.boolProp(BrokerConstants.REAUTHORIZE_SUBSCRIPTIONS_ON_CONNECT, false);
    }
}
