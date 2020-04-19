import {Component} from "@angular/core";
import {
    ComponentLifecycleEventEmitter,
    TupleDataObserverService,
    TupleSelector
} from "@synerty/vortexjs";
import {Ng2BalloonMsgService} from "@synerty/ng2-balloon-msg";
import {AdminStatusTuple} from "../tuples/AdminStatusTuple";


@Component({
    selector: 'pl-livedb-status',
    templateUrl: './status.component.html'
})
export class StatusComponent extends ComponentLifecycleEventEmitter {

    item: AdminStatusTuple = new AdminStatusTuple();

    constructor(private balloonMsg: Ng2BalloonMsgService,
                private tupleObserver: TupleDataObserverService) {
        super();

        let ts = new TupleSelector(AdminStatusTuple.tupleName, {});
        this.tupleObserver.subscribeToTupleSelector(ts)
            .takeUntil(this.onDestroyEvent)
            .subscribe((tuples: AdminStatusTuple[]) => this.item = tuples[0]);

    }


}