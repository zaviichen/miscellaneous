package core;

import quark.proto.TickProtos.Tick;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import util.Util;
import java.util.ArrayList;
import java.util.List;

public class BarGenerator {

    private final static Logger logger = LoggerFactory.getLogger(BarGenerator.class);

    public long interval = 60*1000;
    public String symbol = "";
    public Tick lastTick = null;
    private List<Tick> intervalTicks = new ArrayList<>();
    private Tick.Builder preBar = null;
    private Tick.Builder curBar = null;

    public Tick getCurBar() {
        if (curBar == null)
            return null;
        return curBar.build();
    }

    public BarGenerator(String symbol, int interval) {
        this.symbol = symbol;
        this.interval = interval;
    }

    public void onTick(Tick tick)
    {
        if (tick == null || !tick.getSymbol().equals(symbol)) {
            return;
        }

        if (curBar != null) {
            double px = tick.getLast();
            if (!curBar.hasOpen()) {
                curBar.setOpen(px).setHigh(px).setLow(px);
            } else {
                curBar.setHigh(Math.max(curBar.getHigh(), px));
                curBar.setLow(Math.min(curBar.getLow(), px));
            }
            curBar.setLast(px).setClose(px);

            if (lastTick != null) {
                long deltaVol = tick.getVolume() - lastTick.getVolume();
                curBar.setVolume(curBar.getVolume() + deltaVol);
                double deltaTvr = tick.getTurnover() - lastTick.getTurnover();
                curBar.setTurnover(curBar.getTurnover() + deltaTvr);
            } else {
                curBar.setVolume(0).setTurnover(0);
            }

            curBar.setUpdateUtc(tick.getUpdateUtc());
            curBar.setTradeDate(tick.getTradeDate());
            curBar.setUpdateTime(tick.getUpdateTime());
            curBar.setIsComplete(false);
            intervalTicks.add(tick);
        }

        lastTick = tick;
    }

    private boolean isInit(Tick.Builder bar) {
        return bar != null && bar.hasOpen();
    }

    public void onTimer(long dt)
    {
        if (dt % interval != 0) {
            return;
        }

        // 1. finish the old bar
        if (curBar != null) {
            // no ticks coming during the interval
            if (!isInit(curBar)) {
                logger.info("cur_bar isn't initialized, clone from pre_bar.");
                if (isInit(preBar)) {
                    double px = preBar.getClose();
                    curBar.setOpen(px).setHigh(px).setLow(px).setClose(px).setLast(px);
                    curBar.setVolume(0).setTurnover(0);
                } else {
                    logger.info("pre_bar isn't initialized. do nothing.");
                }
            }
            curBar.setIsComplete(true);
            preBar = curBar;
            logger.info("complete bar - {}: {}", Util.longToDate(preBar.getCreateUtc()), preBar.toString().replace('\n', ','));
        } else {
            logger.info("totally empty. do nothing.");
        }

        // 2. create a new un-initialized bar
        curBar = Tick.newBuilder();
        curBar.setCreateUtc(dt);
        intervalTicks.clear();
    }
}
