require('dotenv').config();
const { Pool } = require("pg");
const { LogicalReplicationService, PgoutputPlugin } = require("pg-logical-replication");
const { sendEmployeeData } = require("./apiService");
const sqlite3 = require('sqlite3').verbose(); // เน€เธเธฅเธตเนเธขเธเธกเธฒเนเธเน sqlite3
const path = require('path');

// ===== 1. SQLITE QUEUE SETUP (Asynchronous) =====
const dbPath = path.join(__dirname, 'queue.db');
const dbQueue = new sqlite3.Database(dbPath);

// เธชเธฃเนเธฒเธ Table (เนเธเน serialize เน€เธเธทเนเธญเนเธซเนเธกเธฑเนเธเนเธเธงเนเธฒเธชเธฃเนเธฒเธเน€เธชเธฃเนเธเธเนเธญเธเธ—เธณเธญเธขเนเธฒเธเธญเธทเนเธ)
dbQueue.serialize(() => {
    dbQueue.run(`
        CREATE TABLE IF NOT EXISTS sync_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            payload TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    `);
});

const addToQueue = (data) => {
    const stmt = dbQueue.prepare('INSERT INTO sync_queue (payload) VALUES (?)');
    stmt.run(JSON.stringify(data), (err) => {
        if (err) console.error("โ Queue Insert Error:", err.message);
    });
    stmt.finalize();
};

// ===== 2. QUEUE PROCESSOR (Logic เน€เธ”เธดเธกเนเธ•เนเธเธฃเธฑเธเน€เธเนเธ Async Callback) =====
let isProcessing = false;

async function processQueue() {
    if (isProcessing) return;

    // เธ”เธถเธเธเนเธญเธกเธนเธฅเนเธ–เธงเธ—เธตเนเน€เธเนเธฒเธ—เธตเนเธชเธธเธ” 1 เนเธ–เธง
    dbQueue.get('SELECT * FROM sync_queue ORDER BY id ASC LIMIT 1', async (err, task) => {
        if (err) {
            console.error("โ Database Error:", err.message);
            isProcessing = false;
            return;
        }

        if (!task) {
            isProcessing = false;
            return; // เนเธกเนเธกเธตเธเธฒเธเธเนเธฒเธเนเธเธเธดเธง
        }

        isProcessing = true;
        let payload;
        try {
            payload = JSON.parse(task.payload);
        } catch (e) {
            // เธ–เนเธฒ JSON เธเธฑเธเนเธซเนเธฅเธเธ—เธดเนเธเน€เธฅเธข
            dbQueue.run('DELETE FROM sync_queue WHERE id = ?', [task.id]);
            isProcessing = false;
            return setImmediate(processQueue);
        }

        try {
            console.log(`๐“ค [Queue] Syncing ID: ${payload.codempid}`);
            await sendEmployeeData(payload);
            
            // เธชเนเธเธชเธณเน€เธฃเนเธ -> เธฅเธเธญเธญเธเธเธฒเธเธเธดเธง
            dbQueue.run('DELETE FROM sync_queue WHERE id = ?', [task.id], (err) => {
                console.log(`โ… [Queue] ID: ${payload.codempid} synced successfully.`);
                isProcessing = false;
                setImmediate(processQueue); // เธ•เธฃเธงเธเธชเธญเธเธเธดเธงเธ–เธฑเธ”เนเธเธ—เธฑเธเธ—เธต
            });
        } catch (err) {
            const status = err.response?.status;
            if (status === 400) {
                console.error(`โ ๏ธ Data Error (400) for ID ${payload.codempid}. Skipping...`);
                dbQueue.run('DELETE FROM sync_queue WHERE id = ?', [task.id]);
                isProcessing = false;
                return setImmediate(processQueue);
            }
            
            // เธเธฃเธ“เธต Error เธญเธทเนเธเน เนเธซเนเธฃเธญ 5 เธเธฒเธ—เธตเธเนเธญเธขเธฅเธญเธเนเธซเธกเนเธ•เธฒเธก Logic เน€เธ”เธดเธก
            isProcessing = false;
            console.log(`โณ Retrying ${payload.codempid} failed. Retrying in 5 minutes...`);
            setTimeout(processQueue, 300000); 
        }
    });
}

// ===== 3. DATABASE CONFIG (POSTGRES) =====
const dbConfig = {
    host: process.env.PG_HOST || "localhost",
    database: process.env.PG_DATABASE || "samart_prd",
    user: process.env.PG_USER || "postgres",
    password: String(process.env.PG_PASSWORD || "Samart@db123"),
    port: Number(process.env.PG_PORT || 5432),
};

const pool = new Pool(dbConfig);
const service = new LogicalReplicationService(dbConfig);
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
const REPLICATION_SLOT = process.env.PG_REPLICATION_SLOT || "emp_sync_slot";
const PUBLICATION_NAME = process.env.PG_PUBLICATION_NAME || "emp_sync_pub";
const CDC_SCHEMA = process.env.PG_CDC_SCHEMA || "dbo";
const TRACKED_TABLES = ["app_users", "contacts", "biographies", "educations"];

const normalizeEducationLevel = (level) => {
    if (level === undefined || level === null || level === "") return null;

    const value = String(level).trim().replace(/\s+/g, " ");

    if (!value) return null;
    if (value.includes("ดุษฎีบัณฑิต") || value.includes("ปริญญาเอก") || value === "ป.เอก") return "ป.เอก";
    if (value.includes("มหาบัณฑิต") || value.includes("ปริญญาโท") || value === "ป.โท") return "ป.โท";
    if (value.includes("บัณฑิต") || value.includes("ปริญญาตรี") || value === "ป.ตรี") return "ป.ตรี";
    if (value.includes("อนุปริญญา")) return "อนุปริญญา";
    if (value.includes("ปวส")) return "ปวส.";
    if (value.includes("ปวช")) return "ปวช.";
    if (value.includes("มัธยมศึกษาตอนปลาย") || value.includes("ม.6")) return "มัธยมศึกษาตอนปลาย";
    if (value.includes("มัธยมศึกษา")) return "มัธยมศึกษา";
    if (value.includes("ประถม")) return "ประถมศึกษา";

    return value;
};

const normalizeEducationGrade = (grade) => {
    if (grade === undefined || grade === null || grade === "") return null;

    const parsed = Number.parseFloat(grade);
    if (Number.isNaN(parsed) || parsed <= 0) return null;

    return parsed;
};

const normalizeEducationYear = (yearValue) => {
    if (yearValue === undefined || yearValue === null || yearValue === "") return null;

    const parsed = Number.parseInt(yearValue, 10);
    if (Number.isNaN(parsed) || parsed <= 0) return null;

    return parsed;
};

const normalizeEducationEntry = (entry) => ({
    educat_lv: normalizeEducationLevel(entry?.educat_lv ?? null),
    educat_institute: entry?.educat_institute ? String(entry.educat_institute).trim() || null : null,
    educat_major: entry?.educat_major ? String(entry.educat_major).trim() || null : null,
    educat_subject: entry?.educat_subject ? String(entry.educat_subject).trim() || null : null,
    educat_grade: normalizeEducationGrade(entry?.educat_grade),
    educat_year_end: normalizeEducationYear(entry?.educat_year_end),
    educat_year_entry: normalizeEducationYear(entry?.educat_year_entry),
    educat_faculty: entry?.educat_faculty ? String(entry.educat_faculty).trim() || null : null,
    educat_country: entry?.educat_country ? String(entry.educat_country).trim() || null : null
});

// ===== 4. QUERY เธ”เธถเธเธเนเธญเธกเธนเธฅเธเธเธฑเธเธเธฒเธ + เธเธฒเธฃเธจเธถเธเธฉเธฒ =====
async function getFullEmployeeData(empCode) {
    const sql = `
        SELECT
            u."employeecode"        AS codempid,
            NULL                    AS appnumber,
            u."email"               AS e_mail,
            u."emptypeid"           AS emptypeid,
            u."prefixt"             AS title_nameth,
            u."fnamet"              AS fnameth,
            u."lnamet"              AS lnameth,
            u."prefixe"             AS title_nameen,
            u."fnamee"              AS fnameen,
            u."lnamee"              AS lnameen,
            REGEXP_REPLACE(c."nickname", '^เธเธธเธ“', '') AS nickname,
            CASE
                WHEN wp."workpositionnamee" LIKE '%:%'
                THEN trim(split_part(wp."workpositionnamee", ':', 1))
                ELSE wp."workpositionnamee"
            END                     AS id_position,
            wp.workpositionname AS position_name,
            CAST(pl."positionleveldesc" AS VARCHAR) AS grade,
            et."emptypecode"       AS wf_code,
            et."emptypename"       AS employee_type_name,
            jl."joblevelname"       AS pl,
            split_part(de."departmentnamee", '|', 1) AS id_org,
            mb."branchcode"         AS codcomp1,
            tt."timetablecode"      AS wp_code,
            COALESCE(NULLIF(TRIM(tt."displayname"), ''), NULLIF(TRIM(tt."timetablename"), ''), tt."timetablecode") AS wp_des,
            TO_CHAR(c."startworkingdate", 'YYYY-MM-DD') AS hiredate,
            b."currentphone"        AS office_tel,
            c."mobile"              AS contact_tel,
            c."citizenid"           AS id_card_number,
            TO_CHAR(c."birthday", 'YYYY-MM-DD') AS birthdate,
            b."bloodgroup"          AS blood,
            CASE WHEN c."sex" = 1 THEN '\u0e0a\u0e32\u0e22' WHEN c."sex" = 2 THEN '\u0e2b\u0e0d\u0e34\u0e07' END AS gender,
            b."religion"            AS religion,
            CASE 
                WHEN b."maritalstatustype" = 1 THEN 'S'
                WHEN b."maritalstatustype" = 2 THEN 'M'
                WHEN b."maritalstatustype" = 3 THEN 'W'
                WHEN b."maritalstatustype" = 4 THEN 'D'
            END                     AS m_status,
            c."address"             AS formaladdress,
            c."address"             AS currentaddress,
            3                       AS status_emp,
            u."updatetime"          AS create_date,
            d."divisioncode"        AS cdiv,
            mw."workplaceid"        AS worklocation,
            mw."workplacename"      AS location_name,
            p1."employeecode"       AS mgr1_id,
            p2."employeecode"       AS mgr2_id

        FROM dbo.app_users u
        LEFT JOIN dbo.mas_divisions d ON d."divisionid" = u."divisionid"
        LEFT JOIN dbo.mas_departments de ON de."departmentid" = u."departmentid"
        LEFT JOIN dbo.mas_work_positions wp ON wp."workpositionid" = u."workpositionid"
        LEFT JOIN dbo.mas_position_levels pl ON pl."positionlevelid" = u."positionlevelid"
        LEFT JOIN dbo.time_tables tt ON tt."timetableid" = u."defaulttimetableid"
        LEFT JOIN dbo.contacts c ON c."userid" = u."userid" 
        LEFT JOIN dbo.biographies b ON b."userid" = u."userid"
        LEFT JOIN dbo.mas_job_levels jl ON jl."joblevelid" = b."joblevelid"
        LEFT JOIN dbo.mas_workplaces mw ON mw."workplaceid" = u."workplaceid"
        LEFT JOIN dbo.mas_employee_types et ON et."emptypeid" = u."emptypeid"
        LEFT JOIN dbo.mas_branches mb ON mb."branchid" = u."branchid"
        LEFT JOIN dbo.app_users p1 ON p1."userid" = u."parentid"
        LEFT JOIN dbo.app_users p2 ON p2."userid" = p1."parentid"

        WHERE u."employeecode" = $1;
    `;
    const res = await pool.query(sql, [empCode]);
    if (!res.rows[0]) return null;

    const eduSql = `
        SELECT
            eq."enumtext" AS educat_lv,
            edu."educationname" AS educat_institute,
            edu."educationlevel" AS educat_major,
            COALESCE(NULLIF(TRIM(edu."branchedu"), ''), NULLIF(TRIM(edu."faculty"), '')) AS educat_subject,
            edu."gradeedu" AS educat_grade,
            edu."yearendedu" AS educat_year_end,
            edu."yearentryedu" AS educat_year_entry,
            edu."faculty" AS educat_faculty,
            edu."countryedu" AS educat_country,
            edu."seqno" AS seqno
        FROM dbo.educations edu
        LEFT JOIN dbo.app_enum_text eq
            ON eq."enumname" = 'enumQualificationEdu'
           AND eq."enumvalue" = edu."qualificationedu"
           AND eq."languageid" = 1
        WHERE edu."userid" = (SELECT "userid" FROM dbo.app_users WHERE "employeecode" = $1 LIMIT 1)
        ORDER BY edu."seqno" DESC, edu."educationid" DESC
    `;

    const educationRes = await pool.query(eduSql, [empCode]);
    const educations = educationRes.rows.map(normalizeEducationEntry);
    const primaryEducation = educations[0] || null;

    return {
        ...res.rows[0],
        educat_lv: primaryEducation?.educat_lv || null,
        educat_institute: primaryEducation?.educat_institute || null,
        educat_major: primaryEducation?.educat_major || null,
        educat_subject: primaryEducation?.educat_subject || null,
        educat_grade: primaryEducation?.educat_grade || null,
        educat_year_end: primaryEducation?.educat_year_end || null,
        educations
    };
}

// ===== 5. CDC LISTENER =====
service.on("data", async (lsn, log) => {
    if (!log?.relation?.name) return;

    if (!TRACKED_TABLES.includes(log.relation.name)) return;
    if (!["insert", "update"].includes(log.tag)) return;

    try {
        console.log(`\n๐”” Change Detected: ${log.relation.name} (${log.tag})`);
        await sleep(500); 

        const row = log.new || log.old;
        if (!row) return;

        let empCode = row.employeecode;

        if (!empCode && row.userid) {
            const r = await pool.query(
                "SELECT employeecode FROM dbo.app_users WHERE userid = $1",
                [row.userid],
            );
            empCode = r.rows[0]?.employeecode;
        }

        if (!empCode) {
            console.warn("โ ๏ธ Could not determine EmployeeCode for this change.");
            return;
        }

        const payload = await getFullEmployeeData(empCode);
        if (!payload) return;

        console.log(`๐“ฅ Added to SQLite Queue: ${empCode}`);
        addToQueue(payload);
        processQueue();

    } catch (err) {
        console.error("โ CDC Error:", err);
    }
});

// ===== 6. START SERVICE =====
const plugin = new PgoutputPlugin({
    protoVersion: 1,
    publicationNames: [PUBLICATION_NAME],
});

service.on("error", (err) => {
    console.error("Logical replication service error:", err);
});

async function ensurePublication() {
    const publicationCheck = await pool.query(
        "SELECT 1 FROM pg_publication WHERE pubname = $1",
        [PUBLICATION_NAME],
    );

    if (publicationCheck.rowCount > 0) {
        return;
    }

    const publicationTables = TRACKED_TABLES
        .map((tableName) => `"${CDC_SCHEMA}"."${tableName}"`)
        .join(", ");

    console.log(`Creating publication "${PUBLICATION_NAME}"...`);
    await pool.query(
        `CREATE PUBLICATION "${PUBLICATION_NAME}" FOR TABLE ${publicationTables}`
    );
}

async function ensureReplicationSlot() {
    const currentDbResult = await pool.query("SELECT current_database() AS name");
    const currentDb = currentDbResult.rows[0].name;
    const slotCheck = await pool.query(
        "SELECT slot_name, plugin, slot_type, database FROM pg_replication_slots WHERE slot_name = $1",
        [REPLICATION_SLOT],
    );

    if (slotCheck.rowCount > 0) {
        const slot = slotCheck.rows[0];

        if (slot.database !== currentDb) {
            throw new Error(
                `Replication slot "${REPLICATION_SLOT}" exists on database "${slot.database}", but this app is connected to "${currentDb}".`
            );
        }

        console.log(
            `Found replication slot "${slot.slot_name}" (${slot.slot_type}/${slot.plugin}) on database "${slot.database}".`
        );
        return;
    }

    console.log(`Creating logical replication slot "${REPLICATION_SLOT}"...`);
    await pool.query(
        "SELECT * FROM pg_create_logical_replication_slot($1, 'pgoutput')",
        [REPLICATION_SLOT],
    );
}

async function start() {
    try {
        const currentDb = await pool.query("SELECT current_database() AS name");
        console.log(`Postgres database: ${currentDb.rows[0].name}`);
        await ensurePublication();
        await ensureReplicationSlot();
        await service.subscribe(plugin, REPLICATION_SLOT);
        processQueue();
        console.log("Worker running with sqlite3 (No-Build dependency) & Education Support...");
        console.log(`Listening with publication "${PUBLICATION_NAME}" and slot "${REPLICATION_SLOT}".`);
    } catch (err) {
        console.error("Failed to start CDC worker:", err);
        process.exitCode = 1;
    }
}

start();



