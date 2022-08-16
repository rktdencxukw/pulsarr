package ai.platon.pulsar.browser.driver.chrome

import ai.platon.pulsar.common.geometric.DimD
import ai.platon.pulsar.common.geometric.OffsetD
import ai.platon.pulsar.common.geometric.PointD
import ai.platon.pulsar.common.geometric.RectD
import ai.platon.pulsar.common.getLogger
import ai.platon.pulsar.common.sleepMillis
import com.github.kklisura.cdt.protocol.commands.DOM
import com.github.kklisura.cdt.protocol.commands.Input
import com.github.kklisura.cdt.protocol.commands.Page
import com.github.kklisura.cdt.protocol.types.input.*
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import org.apache.commons.math3.util.Precision
import kotlin.math.abs
import kotlin.math.max
import kotlin.math.min

internal data class DeviceMetrics(
    val width: Int,
    val height: Int,
    val deviceScaleFactor: Double,
    val mobile: Boolean,
)

data class NodeClip(
    var nodeId: Int = 0,
    var pageX: Int = 0,
    var pageY: Int = 0,
    var rect: RectD? = null,
)

/**
 * ClickableDOM provides a set of methods to help users to click on a specified DOM correctly.
 *
 * @author Vincent Zhang, ivincent.zhang@gmail.com, platon.ai
 */
class ClickableDOM(
    val page: Page,
    val dom: DOM,
    val nodeId: Int,
    val offset: OffsetD? = null
) {
    private val logger = getLogger(this)

    fun clickablePoint(): PointD? {
        val contentQuads = dom.getContentQuads(nodeId, null, null)
        val layoutMetrics = page.layoutMetrics
        if (contentQuads == null || layoutMetrics == null) {
            // throw new Error('Node is either not clickable or not an HTMLElement');
            // return 'error:notvisible';
            logger.info("error:notvisible")
            return null
        }

        val viewport = layoutMetrics.cssLayoutViewport

        val dim = DimD(viewport.clientWidth.toDouble(), viewport.clientHeight.toDouble())
        val quads = contentQuads.filterNotNull()
            .map { fromProtocolQuad(it) }
            .map { intersectQuadWithViewport(it, dim.width, dim.height) }
            .filter { computeQuadArea(it) > 0.99 }
        if (quads.isEmpty()) {
            // throw new Error('Node is either not clickable or not an HTMLElement');
            // return 'error:notinviewport'
            logger.info("error:notinviewport")
            return null
        }

        val quad = quads[0]

        if (offset != null) {
            // Return the point of the first quad identified by offset.
            val MAX_SAFE_POSITION = 1000000.0
            var minX = MAX_SAFE_POSITION
            var minY = MAX_SAFE_POSITION
            for (point in quad) {
                if (point.x < minX) {
                    minX = point.x
                }
                if (point.y < minY) {
                    minY = point.y
                }
            }

            if (!Precision.equals(minX, MAX_SAFE_POSITION) && !Precision.equals(minY, MAX_SAFE_POSITION)) {
                return PointD(x = minX + offset.x, y = minY + offset.y)
            }
        }

        // Return the middle point of the first quad.
        var x = 0.0
        var y = 0.0
        for (point in quad) {
            x += point.x
            y += point.y
        }

        return PointD(x = x / 4, y = y / 4)
    }

    fun boundingBox(): RectD? {
        val box = kotlin.runCatching { dom.getBoxModel(nodeId, null, null) }
            .onFailure { logger.warn("Failed to get box model for #{} | {}", nodeId, it.message) }
            .getOrNull() ?: return null

        val quad = box.border
        if (quad.isEmpty()) {
            return null
        }

        val x = arrayOf(quad[0], quad[2], quad[4], quad[6]).minOrNull()!!
        val y = arrayOf(quad[1], quad[3], quad[5], quad[7]).minOrNull()!!
        val width = arrayOf(quad[0], quad[2], quad[4], quad[6]).maxOrNull()!! - x
        val height = arrayOf(quad[1], quad[3], quad[5], quad[7]).maxOrNull()!! - y

        // TODO: handle iframes

        return RectD(x, y, width, height)
    }

    private fun fromProtocolQuad(quad: List<Double>): List<PointD> {
        return listOf(
            PointD(quad[0], quad[1]),
            PointD(quad[2], quad[3]),
            PointD(quad[4], quad[5]),
            PointD(quad[6], quad[7])
        )
    }

    private fun intersectQuadWithViewport(quad: List<PointD>, width: Double, height: Double): List<PointD> {
        return quad.map { point ->
            PointD(x = min(max(point.x, 0.0), width), y = min(max(point.y, 0.0), height))
        }
    }

    private fun computeQuadArea(quad: List<PointD>): Double {
        /* Compute sum of all directed areas of adjacent triangles
          https://en.wikipedia.org/wiki/Polygon#Simple_polygons
        */
        var area = 0.0

        var i = 0
        while (i < quad.size) {
            val p1 = quad[i]
            val p2 = quad[(i + 1) % quad.size]
            area += (p1.x * p2.y - p2.x * p1.y) / 2;

            ++i
        }

        return abs(area)
    }
}

/**
 * The Mouse class operates in main-frame CSS pixels
 * relative to the top-left corner of the viewport.
 *
 * @author Vincent Zhang, ivincent.zhang@gmail.com, platon.ai
 */
class Mouse(private val input: Input) {
    var currentX = 0.0
    var currentY = 0.0

    /**
     * Shortcut for `mouse.move`, `mouse.down` and `mouse.up`.
     * @param x - Horizontal position of the mouse.
     * @param y - Vertical position of the mouse.
     */
    suspend fun click(x: Double, y: Double, clickCount: Int = 1, delayMillis: Long = 500) {
        move(x, y)
        down(x, y, clickCount)

        if (delayMillis > 0) {
            delay(delayMillis)
        }

        up(x, y, clickCount)
    }

    suspend fun move(point: PointD, steps: Int = 5, delayMillis: Long = 50) {
        move(point.x, point.y, steps, delayMillis)
    }

    suspend fun move(x: Double, y: Double, steps: Int = 5, delayMillis: Long = 50) {
        val fromX = currentX
        val fromY = currentY

        currentX = x
        currentY = y

        var i = 0
        while (i <= steps) {
            val x1 = fromX + (currentX - fromX) * (i.toDouble() / steps)
            val y1 = fromY + (currentY - fromY) * (i.toDouble() / steps)

            input.dispatchMouseEvent(
                DispatchMouseEventType.MOUSE_MOVED, x1, y1,
                null, null,
                null, // button
                null, // buttons
                null,
                null, // force
                null,
                null,
                null,
                null, // twist
                null,
                null,
                null
            )

            if (delayMillis > 0) {
                delay(delayMillis)
            }

            ++i
        }
    }

    /**
     * Dispatches a `mousedown` event.
     */
    suspend fun down(clickCount: Int = 1) {
        down(currentX, currentY, clickCount)
    }

    /**
     * Dispatches a `mousedown` event.
     */
    suspend fun down(point: PointD, clickCount: Int = 1) {
        down(point.x, point.y, clickCount)
    }

    /**
     * Dispatches a `mousedown` event.
     */
    suspend fun down(x: Double, y: Double, clickCount: Int = 1) {
        withContext(Dispatchers.IO) {
            input.dispatchMouseEvent(
                DispatchMouseEventType.MOUSE_PRESSED, x, y,
                null, null,
                MouseButton.LEFT,
                null, // buttons
                clickCount,
                0.5, // force
                null,
                null,
                null,
                null, // twist
                null,
                null,
                null
            )
        }
    }

    suspend fun up() {
        up(currentX, currentY)
    }

    suspend fun up(point: PointD) {
        up(point.x, point.y)
    }

    suspend fun up(x: Double, y: Double, clickCount: Int = 1) {
        withContext(Dispatchers.IO) {
            input.dispatchMouseEvent(
                DispatchMouseEventType.MOUSE_RELEASED, x, y,
                null, null,
                MouseButton.LEFT,
                null, // buttons
                clickCount,
                null, // force
                null,
                null, // tiltX
                null, // tiltY
                null, // twist
                null, // deltaX
                null, // deltaY
                null
            )
        }
    }

    /**
     * Dispatches a `mousewheel` event.
     *
     * @example
     * An example of zooming into an element:
     * ```
     * val elem = driver.querySelector('div');
     * val boundingBox = elem.boundingBox();
     * mouse.move(
     *   boundingBox.x + boundingBox.width / 2,
     *   boundingBox.y + boundingBox.height / 2
     * );
     *
     * mouse.wheel({ deltaY: -100 })
     * ```
     */
    suspend fun wheel(x: Double, y: Double, deltaX: Double, deltaY: Double) {
        withContext(Dispatchers.IO) {
            input.dispatchMouseEvent(
                DispatchMouseEventType.MOUSE_WHEEL, x, y,
                null, null,
                null, // button
                null, // buttons
                null,
                null, // force
                null,
                null, // tiltX
                null, // tiltY
                null, // twist
                deltaX, // deltaX
                deltaY, // deltaY
                null
            )
        }
    }

    /**
     * Dispatches a `drag` event.
     * @param start - starting point for drag
     * @param target - point to drag to
     */
    suspend fun drag(start: PointD, end: PointD): DragData? {
        var dragData: DragData? = null

        withContext(Dispatchers.IO) {
            input.setInterceptDrags(true)
            input.onDragIntercepted {
                dragData = it.data
            }
        }

        move(start, 5, 100)
        down(currentX, currentY)
        move(end, 3, 500)

        return dragData
    }

    /**
     * Dispatches a `dragenter` event.
     * @param target - point for emitting `dragenter` event
     * @param data - drag data containing items and operations mask
     */
    suspend fun dragEnter(target: PointD, data: DragData) {
        withContext(Dispatchers.IO) {
            input.dispatchDragEvent(
                DispatchDragEventType.DRAG_ENTER, target.x, target.y,
                data
            )
        }
    }

    /**
     * Dispatches a `dragover` event.
     * @param target - point for emitting `dragover` event
     * @param data - drag data containing items and operations mask
     */
    suspend fun dragOver(target: PointD, data: DragData) {
        withContext(Dispatchers.IO) {
            input.dispatchDragEvent(
                DispatchDragEventType.DRAG_OVER, target.x, target.y,
                data
            )
        }
    }

    /**
     * Performs a dragenter, dragover, and drop in sequence.
     * @param target - point to drop on
     * @param data - drag data containing items and operations mask
     */
    suspend fun drop(target: PointD, data: DragData) {
        withContext(Dispatchers.IO) {
            input.dispatchDragEvent(
                DispatchDragEventType.DROP, target.x, target.y,
                data
            )
        }
    }

    /**
     * Performs a drag, dragenter, dragover, and drop in sequence.
     * @param target - point to drag from
     * @param target - point to drop on
     * @param options - An object of options. Accepts delay which,
     * if specified, is the time to wait between `dragover` and `drop` in milliseconds.
     * Defaults to 0.
     */
    suspend fun dragAndDrop(start: PointD, target: PointD, delayMillis: Long = 500) {
        val data = drag(start, target) ?: return
        dragEnter(target, data)
        dragOver(target, data)
        if (delayMillis > 0) {
            delay(delayMillis)
        }
        drop(target, data)
        up()
    }
}

/**
 * Keyboard provides an api for managing a virtual keyboard.
 * */
class Keyboard(private val input: Input) {

    suspend fun type(nodeId: Int, text: String, delayMillis: Long) {
        text.forEach { char ->
            if (Character.isISOControl(char)) {
                // TODO:
            } else {
                input.insertText("$char")
            }
            delay(delayMillis)
        }
    }

    suspend fun press(key: String, delayMillis: Long) {
        down(key)
        delay(delayMillis)
        up(key)
    }

    fun down(key: String) {
        input.dispatchKeyEvent(
            DispatchKeyEventType.KEY_DOWN
        )
    }

    fun up(key: String) {
        input.dispatchKeyEvent(
            DispatchKeyEventType.KEY_UP
        )
    }
}

class Touchscreen() {

}
