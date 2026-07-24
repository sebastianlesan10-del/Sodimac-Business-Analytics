//=========================================================
// INICIALIZACIÓN
//=========================================================

console.log(
    "Javascript cargado"
);

let tipoAuditoriaActual = "";


//=========================================================
// MODAL AUDITORÍA
//=========================================================

const modal =
    document.getElementById(
        "modal_auditoria"
    );

const titulo_modal =
    document.getElementById(
        "titulo_modal"
    );

const resumen_modal =
    document.getElementById(
        "resumen_modal"
    );

const tabla_body =
    document.querySelector(
        "#tabla_auditoria tbody"
    );


//=========================================================
// EVENTOS MODAL
//=========================================================

document
.getElementById(
    "cerrar_modal"
)
.addEventListener(
    "click",
    () => {

        modal.style.display =
            "none";

    }
);

document
.getElementById(
    "cerrar_modal_2"
)
.addEventListener(
    "click",
    () => {

        modal.style.display =
            "none";

    }
);

document
.getElementById(
    "descargar_excel"
)
.addEventListener(
    "click",
    () => {

        window.open(
            `/auditoria/excel/${tipoAuditoriaActual}`,
            "_blank"
        );

    }
);


//=========================================================
// FUNCIONES AUDITORÍA
//=========================================================

function mostrarModalAuditoria(
    tipo,
    resultado
){

    tipoAuditoriaActual =
        tipo;

    titulo_modal.textContent =
        `Auditoría ${tipo}`;

    resumen_modal.textContent =
        `Pendientes encontrados: ${resultado.total_pendientes}`;

    tabla_body.innerHTML = "";

    if(
        resultado.detalle
    ){

        resultado.detalle.forEach(
            fila => {

                tabla_body.innerHTML += `
                    <tr>
                        <td>${fila.NUM_RESERVA}</td>
                        <td>${fila.SKU}</td>
                        <td>${fila.SUB_TIPO_SERVICIO}</td>
                        <td>${fila.FECHA_PACTADA_RESERVA}</td>
                    </tr>
                `;

            }
        );

    }

    modal.style.display =
        "flex";

}


async function ejecutarAuditoria(
    tipo
){

    try {

        const respuesta =
            await fetch(
                `/auditoria/${tipo}`
            );

        const resultado =
            await respuesta.json();

        mostrarModalAuditoria(
            tipo,
            resultado
        );

    }

    catch(error){

        console.error(
            error
        );

        alert(
            "Error al ejecutar auditoría"
        );

    }

}

//=========================================================
// FUNCIONES AUXILIARES CARGA DE ARCHIVOS
//=========================================================

function esperar(ms){

    return new Promise(
        resolve => setTimeout(
            resolve,
            ms
        )
    );

}

function cambiarEstado(
    etapa,
    estado
){

    const colores = {

        pendiente: "",
        proceso: "orange",
        ok: "green",
        error: "red"

    };

    etapa.style.backgroundColor =
        colores[estado];

}


//=========================================================
// FUNCIONES CARGA DE ARCHIVOS
//=========================================================

function inicializarCarga(
    tipo,
    endpoint
){

    const archivo =
        document.getElementById(
            `archivo_${tipo}`
        );

    const label =
        document.getElementById(
            `label_archivo_${tipo}`
        );

    const subir =
        document.getElementById(
            `subir_${tipo}`
        );

    const progreso =
        document.getElementById(
            `progreso_carga_${tipo}`
        );

    const recibido =
        document.getElementById(
            `etapa_recibido_${tipo}`
        );

    const validacion =
        document.getElementById(
            `etapa_validacion_${tipo}`
        );

    const sql =
        document.getElementById(
            `etapa_sql_${tipo}`
        );
    
    const bigquery =
    document.getElementById(
        `etapa_bigquery_${tipo}`
    );

    const finalizado =
        document.getElementById(
            `etapa_finalizado_${tipo}`
        );

    subir.disabled = true;


  
    
    //-----------------------------------------------------
    // SELECCIÓN DE ARCHIVO
    //-----------------------------------------------------

    archivo.addEventListener(
        "change",
        () => {

            recibido.style.backgroundColor = "";
            validacion.style.backgroundColor = "";
            sql.style.backgroundColor = "";
            bigquery.style.backgroundColor = "";
            finalizado.style.backgroundColor = "";


            label.textContent =
                archivo.files[0].name;

            progreso.style.width =
                "0%";

            let porcentaje = 0;

            const carga =
                setInterval(
                    () => {

                        porcentaje += 10;

                        progreso.style.width =
                            porcentaje + "%";

                        if(
                            porcentaje >= 100
                        ){

                            clearInterval(
                                carga
                            );

                        }

                    },
                    150
                );

            subir.disabled =
                false;

        }
    );


    //-----------------------------------------------------
    // SUBIR ARCHIVO
    //-----------------------------------------------------

    subir.addEventListener(
        "click",
        async () => {

            if(
                archivo.files.length === 0
            ){

                alert(
                    "Seleccione un archivo"
                );

                return;

            }

            try {
                
                cambiarEstado(
                    recibido,
                    "proceso"
                );

                await esperar(500);

                cambiarEstado(
                    recibido,
                    "ok"
                );

                cambiarEstado(
                    validacion,
                    "proceso"
                );

                await esperar(700);

                cambiarEstado(
                    validacion,
                    "ok"
                );

                cambiarEstado(
                    sql,
                    "proceso"
                );

                subir.disabled =
                    true;

                const formData =
                    new FormData();

                formData.append(
                    "archivo",
                    archivo.files[0]
                );

                const respuesta =
                    await fetch(
                        endpoint,
                        {
                            method: "POST",
                            body: formData
                        }
                    );

                const resultado =
                    await respuesta.json();

                if(
                    resultado.estado === "ok"
                ){

                    alert(
                        `Carga exitosa\nRegistros cargados: ${resultado.registros}`
                    );

                    
                    cambiarEstado(
                        sql,
                        "ok"
                    );

                    cambiarEstado(
                        bigquery,
                        "proceso"
                    );

                    await esperar(800);

                    cambiarEstado(
                        bigquery,
                        "ok"
                    );

                    cambiarEstado(
                        finalizado,
                        "proceso"
                    );

                    await esperar(400);

                    cambiarEstado(
                        finalizado,
                        "ok"
                    );  
                }

                else {

                    alert(
                        `Error: ${resultado.mensaje}`
                    );

                    cambiarEstado(
                        validacion,
                        "error"
                    );


                }

            }

            catch(error){

                console.error(
                    error
                );

                alert(
                    "Error al comunicarse con el servidor"
                );

                cambiarEstado(
                    validacion,
                    "error"
                );


            }

        }
    );

}


//=========================================================
// FUNCIONES AUDITORÍA POR BLOQUE
//=========================================================

function inicializarAuditoria(
    tipoHtml,
    tipoBackend
){

    const boton =
        document.getElementById(
            `auditar_${tipoHtml}`
        );

    boton.addEventListener(
        "click",
        () => {

            ejecutarAuditoria(
                tipoBackend
            );

        }
    );

}


//=========================================================
// DADC
//=========================================================

inicializarCarga(
    "dadc",
    "/cargar_dadc"
);

inicializarAuditoria(
    "dadc",
    "DADc"
);


//=========================================================
// RT
//=========================================================

inicializarCarga(
    "rt",
    "/cargar_rt"
);

inicializarAuditoria(
    "rt",
    "RT"
);


//=========================================================
// DADPIS
//=========================================================

inicializarCarga(
    "dadpis",
    "/cargar_dadpis"
);

inicializarAuditoria(
    "dadpis",
    "DADPis"
);